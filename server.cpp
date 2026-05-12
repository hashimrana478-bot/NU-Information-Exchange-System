// ============================================================
//  NU-Information Exchange System
//  Central Server - Islamabad Campus
//  FAST-NUCES Multi-Campus Network
//
//  Features:
//    - TCP multi-threaded client handling (one thread per campus)
//    - UDP heartbeat monitoring with last-seen timestamps
//    - UDP system-wide broadcast from Admin console
//    - Campus authentication (hard-coded credentials)
//    - Inter-campus TCP message routing
//    - Admin dashboard: connected clients + last-seen status
//    - Graceful disconnect + cleanup
//    - Full console logging with timestamps
//    - Bonus: File transfer support over TCP
// ============================================================

#include <winsock2.h>
#include <ws2tcpip.h>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <mutex>
#include <map>
#include <vector>
#include <ctime>
#include <iomanip>
#include <fstream>
#include <algorithm>

#pragma comment(lib, "ws2_32.lib")

// ---- Network Configuration ----
#define TCP_PORT       9000
#define UDP_PORT       9001
#define MAX_CLIENTS    10
#define BUFFER_SIZE    4096
#define HEARTBEAT_TIMEOUT 30   // seconds before campus marked offline

// ---- Hard-coded Campus Credentials ----
// Format: campusName -> password
std::map<std::string, std::string> validCampuses = {
    {"Islamabad", "NU-ISB-123"},
    {"Lahore",    "NU-LHR-123"},
    {"Karachi",   "NU-KHI-123"},
    {"Peshawar",  "NU-PSH-123"},
    {"CFD",       "NU-CFD-123"},
    {"Multan",    "NU-MUL-123"}
};

// ---- Shared State ----
std::map<std::string, SOCKET>     connectedClients;   // campusName -> TCP socket
std::map<std::string, sockaddr_in> udpClients;         // campusName -> UDP address
std::map<std::string, time_t>     lastSeen;            // campusName -> last heartbeat time
std::map<std::string, std::string> campusStatus;       // campusName -> status string
std::mutex clientsMutex;                               // protects all shared maps

SOCKET udpSocket;   // global UDP socket (used by admin broadcast)

// ============================================================
//  Utility: get current timestamp as string [HH:MM:SS]
// ============================================================
std::string timestamp() {
    time_t now = time(nullptr);
    struct tm t;
    localtime_s(&t, &now);
    char buf[16];
    strftime(buf, sizeof(buf), "[%H:%M:%S]", &t);
    return std::string(buf);
}

// ============================================================
//  Utility: log a message to console with timestamp and tag
// ============================================================
void log(const std::string& tag, const std::string& msg) {
    std::cout << timestamp() << " [" << tag << "] " << msg << "\n";
}

// ============================================================
//  Utility: split a string by a delimiter into a vector
// ============================================================
std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> tokens;
    std::string tok;
    std::istringstream ss(s);
    while (std::getline(ss, tok, delim))
        tokens.push_back(tok);
    return tokens;
}

// ============================================================
//  Authentication
//  Expected message format: AUTH|Campus:Lahore,Pass:NU-LHR-123
//  Returns campus name on success, empty string on failure.
// ============================================================
std::string authenticate(const std::string& msg) {
    if (msg.substr(0, 5) != "AUTH|") return "";

    std::string body = msg.substr(5);

    size_t c1 = body.find("Campus:");
    size_t c2 = body.find(",Pass:");
    if (c1 == std::string::npos || c2 == std::string::npos) return "";

    std::string campus = body.substr(c1 + 7, c2 - (c1 + 7));
    std::string pass   = body.substr(c2 + 6);

    // Trim whitespace/CR/LF
    campus.erase(std::remove_if(campus.begin(), campus.end(),
        [](char c){ return c == '\r' || c == '\n'; }), campus.end());
    pass.erase(std::remove_if(pass.begin(), pass.end(),
        [](char c){ return c == '\r' || c == '\n'; }), pass.end());

    if (validCampuses.count(campus) && validCampuses[campus] == pass)
        return campus;
    return "";
}

// ============================================================
//  Message Routing (TCP)
//  Format: MSG|SrcCampus|SrcDept|DstCampus|DstDept|Text
//  Parses destination campus and forwards the full message
//  to that campus's TCP socket.
// ============================================================
void routeMessage(const std::string& msg, const std::string& srcCampus) {
    auto parts = split(msg, '|');
    // Expected: [0]=MSG [1]=SrcCampus [2]=SrcDept [3]=DstCampus [4]=DstDept [5]=Text
    if (parts.size() < 6) {
        log("ROUTE", "Malformed MSG from " + srcCampus + " (too few fields)");
        return;
    }

    std::string dstCampus = parts[3];

    std::lock_guard<std::mutex> lock(clientsMutex);
    if (connectedClients.count(dstCampus)) {
        SOCKET dstSock = connectedClients[dstCampus];
        int sent = send(dstSock, msg.c_str(), (int)msg.size(), 0);
        if (sent == SOCKET_ERROR)
            log("ROUTE", "Send error to " + dstCampus + " (WSA: " + std::to_string(WSAGetLastError()) + ")");
        else
            log("ROUTE", srcCampus + " -> " + dstCampus
                + " [" + parts[2] + " -> " + parts[4] + "] \"" + parts[5] + "\"");
    } else {
        // Destination campus not connected - notify sender
        std::string err = "ERR|Campus " + dstCampus + " is not currently connected.";
        if (connectedClients.count(srcCampus))
            send(connectedClients[srcCampus], err.c_str(), (int)err.size(), 0);
        log("ROUTE", "Destination campus '" + dstCampus + "' not connected");
    }
}

// ============================================================
//  File Transfer Routing (TCP)
//  Format: FILE|SrcCampus|DstCampus|Filename|FileSize\n<binary>
//  The full packet (header + binary) arrives in the message
//  buffer. Server parses header, finds destination socket,
//  then forwards the entire raw packet + remaining bytes.
// ============================================================
void routeFile(const std::string& fullPacket, int packetLen, SOCKET srcSock, const std::string& srcCampus) {
    // Find header delimiter
    size_t nlPos = fullPacket.find('\n');
    if (nlPos == std::string::npos) {
        log("FILE", "Malformed FILE packet from " + srcCampus + " (no delimiter)");
        return;
    }

    std::string header = fullPacket.substr(0, nlPos);
    auto parts = split(header, '|');
    // parts: [0]=FILE [1]=SrcCampus [2]=DstCampus [3]=Filename [4]=FileSize
    if (parts.size() < 5) {
        log("FILE", "Malformed FILE header from " + srcCampus);
        return;
    }

    std::string dstCampus = parts[2];
    std::string filename  = parts[3];
    long        fileSize  = 0;
    try { fileSize = std::stol(parts[4]); } catch (...) {
        log("FILE", "Invalid file size in header from " + srcCampus);
        return;
    }

    log("FILE", srcCampus + " -> " + dstCampus + " | " + filename
        + " (" + std::to_string(fileSize) + " bytes)");

    // Find destination socket
    SOCKET dstSock = INVALID_SOCKET;
    {
        std::lock_guard<std::mutex> lock(clientsMutex);
        if (connectedClients.count(dstCampus))
            dstSock = connectedClients[dstCampus];
    }

    if (dstSock == INVALID_SOCKET) {
        std::string err = "ERR|File transfer failed: campus '" + dstCampus + "' is not connected.";
        send(srcSock, err.c_str(), (int)err.size(), 0);
        log("FILE", "Destination '" + dstCampus + "' not connected");
        return;
    }

    // Forward whatever we already have in the buffer (header + partial/full file data)
    send(dstSock, fullPacket.c_str(), packetLen, 0);

    // Calculate how many file bytes were already in first packet
    long headerLen   = (long)(nlPos + 1);
    long alreadyFwd  = (long)packetLen - headerLen;
    long remaining   = fileSize - alreadyFwd;

    // Relay any remaining file bytes directly src -> dst
    char buf[BUFFER_SIZE];
    while (remaining > 0) {
        int toRead = (int)std::min((long)BUFFER_SIZE, remaining);
        int got    = recv(srcSock, buf, toRead, 0);
        if (got <= 0) break;
        send(dstSock, buf, got, 0);
        remaining -= got;
    }

    std::string ack = "ACK|File '" + filename + "' (" + std::to_string(fileSize)
                    + " bytes) delivered to " + dstCampus + " successfully.";
    send(srcSock, ack.c_str(), (int)ack.size(), 0);
    log("FILE", "Transfer complete: " + filename + " -> " + dstCampus);
}

// ============================================================
//  Campus Client Thread
//  Each connected campus runs in its own thread.
//  Lifecycle: AUTH -> main message loop -> cleanup on disconnect
// ============================================================
void handleClient(SOCKET clientSock) {
    char buffer[BUFFER_SIZE];
    std::string campusName = "";

    // ---- Step 1: Authentication ----
    int bytesReceived = recv(clientSock, buffer, BUFFER_SIZE - 1, 0);
    if (bytesReceived <= 0) {
        closesocket(clientSock);
        return;
    }
    buffer[bytesReceived] = '\0';
    std::string authMsg(buffer);

    campusName = authenticate(authMsg);
    if (campusName.empty()) {
        std::string reject = "AUTH_FAIL|Invalid campus name or password.";
        send(clientSock, reject.c_str(), (int)reject.size(), 0);
        log("AUTH", "FAILED - invalid credentials");
        closesocket(clientSock);
        return;
    }

    // Check for duplicate connection
    {
        std::lock_guard<std::mutex> lock(clientsMutex);
        if (connectedClients.count(campusName)) {
            std::string dup = "AUTH_FAIL|Campus " + campusName + " is already connected.";
            send(clientSock, dup.c_str(), (int)dup.size(), 0);
            log("AUTH", campusName + " - duplicate connection rejected");
            closesocket(clientSock);
            return;
        }
        connectedClients[campusName] = clientSock;
        campusStatus[campusName]     = "Connected (TCP)";
        lastSeen[campusName]         = time(nullptr);
    }

    std::string ok = "AUTH_OK|Welcome, " + campusName + " campus!";
    send(clientSock, ok.c_str(), (int)ok.size(), 0);
    log("AUTH", campusName + " authenticated and connected");

    // ---- Step 2: Message Loop ----
    while (true) {
        memset(buffer, 0, BUFFER_SIZE);
        bytesReceived = recv(clientSock, buffer, BUFFER_SIZE - 1, 0);
        if (bytesReceived <= 0) {
            log("DISCONNECT", campusName + " has disconnected");
            break;
        }
        buffer[bytesReceived] = '\0';
        std::string message(buffer);

        // Update last-seen on any TCP activity
        {
            std::lock_guard<std::mutex> lock(clientsMutex);
            lastSeen[campusName] = time(nullptr);
        }

        // ---- Dispatch by message type ----
        if (message.substr(0, 4) == "MSG|") {
            routeMessage(message, campusName);

        } else if (message.substr(0, 5) == "FILE|") {
            routeFile(message, bytesReceived, clientSock, campusName);

        } else if (message.substr(0, 5) == "PING|") {
            // Lightweight TCP keep-alive from client
            std::string pong = "PONG|" + campusName;
            send(clientSock, pong.c_str(), (int)pong.size(), 0);

        } else {
            log("MSG", "Unknown message type from " + campusName + ": " + message);
        }
    }

    // ---- Step 3: Cleanup ----
    {
        std::lock_guard<std::mutex> lock(clientsMutex);
        connectedClients.erase(campusName);
        campusStatus.erase(campusName);
        // Keep lastSeen for reference in dashboard until next heartbeat
    }
    closesocket(clientSock);
}

// ============================================================
//  UDP Thread
//  Receives UDP datagrams from campus clients.
//  Handles:
//    HEARTBEAT|CampusName|StatusText  - status update
// ============================================================
void udpThread() {
    char buffer[BUFFER_SIZE];
    sockaddr_in clientAddr;
    int clientSize = sizeof(clientAddr);

    log("UDP", "Heartbeat listener started on port " + std::to_string(UDP_PORT));

    while (true) {
        memset(buffer, 0, BUFFER_SIZE);
        int bytes = recvfrom(udpSocket, buffer, BUFFER_SIZE - 1, 0,
                             (sockaddr*)&clientAddr, &clientSize);
        if (bytes <= 0) continue;

        buffer[bytes] = '\0';
        std::string msg(buffer);

        // Format: HEARTBEAT|CampusName|StatusText
        if (msg.substr(0, 10) == "HEARTBEAT|") {
            auto parts = split(msg, '|');
            if (parts.size() >= 2) {
                std::string campus = parts[1];
                std::string status = (parts.size() >= 3) ? parts[2] : "online";

                std::lock_guard<std::mutex> lock(clientsMutex);
                udpClients[campus]   = clientAddr;
                lastSeen[campus]     = time(nullptr);
                campusStatus[campus] = status;

                log("HEARTBEAT", campus + " [" + status + "]");
            }
        }
    }
}

// ============================================================
//  Admin Thread
//  Console interface for the server administrator.
//  Commands:
//    1 - show dashboard (connected campuses + status)
//    2 - broadcast a message via UDP to all campuses
//    3 - list message routing log
//    0 - exit
// ============================================================
void adminThread() {
    std::this_thread::sleep_for(std::chrono::milliseconds(500)); // let server print its banner first

    while (true) {
        std::cout << "\n+======================================+\n";
        std::cout <<   "|        ADMIN CONSOLE - NU SERVER     |\n";
        std::cout <<   "+======================================+\n";
        std::cout <<   "|  1. Show Campus Dashboard             |\n";
        std::cout <<   "|  2. Broadcast Announcement (UDP)      |\n";
        std::cout <<   "|  0. Shutdown Server                   |\n";
        std::cout <<   "+======================================+\n";
        std::cout << "Admin> ";

        std::string input;
        std::getline(std::cin, input);
        if (input.empty()) continue;

        int choice = 0;
        try { choice = std::stoi(input); } catch (...) { continue; }

        if (choice == 1) {
            // ---- Campus Dashboard ----
            std::lock_guard<std::mutex> lock(clientsMutex);
            time_t now = time(nullptr);

            std::cout << "\n+---------------------------------------------------------+\n";
            std::cout <<   "|               CAMPUS STATUS DASHBOARD                   |\n";
            std::cout <<   "+--------------+--------------+--------------+------------+\n";
            std::cout <<   "| Campus       | TCP          | Last Heartbt | Status     |\n";
            std::cout <<   "+--------------+--------------+--------------+------------+\n";

            for (auto& pair : validCampuses) {
                std::string name = pair.first;
                bool tcpConn  = connectedClients.count(name) > 0;
                bool udpSeen  = lastSeen.count(name) > 0;

                std::string tcpStr   = tcpConn ? "CONNECTED   " : "OFFLINE     ";
                std::string lastSeenStr = "Never       ";
                std::string statusStr   = "OFFLINE";

                if (udpSeen) {
                    int secs = (int)(now - lastSeen[name]);
                    if (secs < 60)
                        lastSeenStr = std::to_string(secs) + "s ago      ";
                    else
                        lastSeenStr = std::to_string(secs / 60) + "m ago      ";

                    statusStr = (secs < HEARTBEAT_TIMEOUT) ? "ONLINE " : "STALE  ";
                }

                // Truncate/pad strings for alignment
                auto pad = [](std::string s, int w) {
                    if ((int)s.size() > w) s = s.substr(0, w);
                    while ((int)s.size() < w) s += ' ';
                    return s;
                };

                std::cout << "| " << pad(name, 12) << " | " << pad(tcpStr, 12)
                          << " | " << pad(lastSeenStr, 12) << " | " << pad(statusStr, 10) << "|\n";
            }
            std::cout << "+--------------+--------------+--------------+------------+\n";
            std::cout << "TCP connected: " << connectedClients.size() << " campus(es)\n";

        } else if (choice == 2) {
            // ---- UDP Broadcast ----
            std::cout << "Enter announcement text: ";
            std::string announcement;
            std::getline(std::cin, announcement);
            if (announcement.empty()) continue;

            std::string broadcast = "BROADCAST|" + announcement;
            int sent = 0;

            std::lock_guard<std::mutex> lock(clientsMutex);
            for (auto& pair : udpClients) {
                int r = sendto(udpSocket, broadcast.c_str(), (int)broadcast.size(), 0,
                               (sockaddr*)&pair.second, sizeof(pair.second));
                if (r != SOCKET_ERROR) sent++;
            }
            log("BROADCAST", "Sent to " + std::to_string(sent) + " campus(es): " + announcement);

        } else if (choice == 0) {
            log("SERVER", "Admin initiated shutdown");
            std::cout << "Shutting down server...\n";
            exit(0);
        }
    }
}

// ============================================================
//  MAIN
// ============================================================
int main() {
    std::cout << "\n";
    std::cout << "+====================================================+\n";
    std::cout << "|     NU-Information Exchange System - SERVER        |\n";
    std::cout << "|     FAST-NUCES Central Hub (Islamabad Campus)      |\n";
    std::cout << "+====================================================+\n\n";

    // ---- Initialize Winsock ----
    WSADATA wsaData;
    int wsaResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (wsaResult != 0) {
        std::cerr << "[FATAL] WSAStartup failed: " << wsaResult << "\n";
        return 1;
    }

    // ---- Create TCP Socket ----
    SOCKET tcpSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (tcpSocket == INVALID_SOCKET) {
        std::cerr << "[FATAL] TCP socket creation failed\n";
        WSACleanup(); return 1;
    }

    // Allow port reuse (helpful during development restarts)
    int opt = 1;
    setsockopt(tcpSocket, SOL_SOCKET, SO_REUSEADDR, (char*)&opt, sizeof(opt));

    sockaddr_in serverAddr{};
    serverAddr.sin_family      = AF_INET;
    serverAddr.sin_port        = htons(TCP_PORT);
    serverAddr.sin_addr.s_addr = INADDR_ANY;

    if (bind(tcpSocket, (sockaddr*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
        std::cerr << "[FATAL] TCP bind failed (port " << TCP_PORT << " in use?)\n";
        closesocket(tcpSocket); WSACleanup(); return 1;
    }
    listen(tcpSocket, MAX_CLIENTS);
    log("OK", "TCP listening on port " + std::to_string(TCP_PORT));

    // ---- Create UDP Socket ----
    udpSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (udpSocket == INVALID_SOCKET) {
        std::cerr << "[FATAL] UDP socket creation failed\n";
        closesocket(tcpSocket); WSACleanup(); return 1;
    }

    sockaddr_in udpAddr{};
    udpAddr.sin_family      = AF_INET;
    udpAddr.sin_port        = htons(UDP_PORT);
    udpAddr.sin_addr.s_addr = INADDR_ANY;

    if (bind(udpSocket, (sockaddr*)&udpAddr, sizeof(udpAddr)) == SOCKET_ERROR) {
        std::cerr << "[FATAL] UDP bind failed (port " << UDP_PORT << " in use?)\n";
        closesocket(tcpSocket); closesocket(udpSocket); WSACleanup(); return 1;
    }
    log("OK", "UDP listening on port " + std::to_string(UDP_PORT));

    // ---- Start Background Threads ----
    std::thread(udpThread).detach();
    std::thread(adminThread).detach();

    log("READY", "Server is online. Waiting for campus connections...\n");

    // ---- TCP Accept Loop ----
    while (true) {
        sockaddr_in clientAddr{};
        int clientSize = sizeof(clientAddr);
        SOCKET clientSock = accept(tcpSocket, (sockaddr*)&clientAddr, &clientSize);

        if (clientSock == INVALID_SOCKET) {
            log("WARN", "accept() error: " + std::to_string(WSAGetLastError()));
            continue;
        }

        // Log incoming IP
        char ipStr[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &clientAddr.sin_addr, ipStr, sizeof(ipStr));
        log("CONNECT", "Incoming connection from " + std::string(ipStr));

        // Spawn a dedicated thread for this campus client
        std::thread(handleClient, clientSock).detach();
    }

    // Cleanup (unreachable in normal flow; admin exit() is used)
    closesocket(tcpSocket);
    closesocket(udpSocket);
    WSACleanup();
    return 0;
}