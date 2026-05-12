// ============================================================
//  NU-Information Exchange System
//  Campus Client Application
//  FAST-NUCES Multi-Campus Network
//
//  Usage:  client.exe <CampusName>
//  Example: client.exe Lahore
//
//  Features:
//    - TCP connection + authentication with Central Server
//    - UDP heartbeat (every 10 seconds) to signal online status
//    - UDP listener for server broadcast announcements
//    - Send direct messages to any campus/department via TCP
//    - Inbox: store and view received messages
//    - Bonus: Send a text file to another campus over TCP
//    - Graceful shutdown and reconnect handling
// ============================================================

#include <winsock2.h>
#include <ws2tcpip.h>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <atomic>
#include <mutex>
#include <vector>
#include <fstream>
#include <ctime>
#include <iomanip>
#include <algorithm>

#pragma comment(lib, "ws2_32.lib")

// ---- Server Configuration ----
#define SERVER_IP    "127.0.0.1"   // Change to server's IP for LAN testing
#define TCP_PORT     9000
#define UDP_PORT     9001
#define BUFFER_SIZE  4096
#define HEARTBEAT_INTERVAL 10      // seconds between UDP heartbeats

// ============================================================
//  Inbox entry: one received message
// ============================================================
struct InboxMessage {
    std::string from;       // source campus
    std::string fromDept;   // source department
    std::string toDept;     // destination department
    std::string text;       // message body
    std::string timeStr;    // timestamp string
};

// ---- Global State ----
std::string            campusName;
SOCKET                 tcpSocket;
SOCKET                 udpSocket;
std::atomic<bool>      running(true);
std::vector<InboxMessage> inbox;          // received messages
std::mutex             inboxMutex;        // protect inbox

// ============================================================
//  Utility: campus password lookup
// ============================================================
std::string getCampusPassword(const std::string& campus) {
    if (campus == "Islamabad") return "NU-ISB-123";
    if (campus == "Lahore")    return "NU-LHR-123";
    if (campus == "Karachi")   return "NU-KHI-123";
    if (campus == "Peshawar")  return "NU-PSH-123";
    if (campus == "CFD")       return "NU-CFD-123";
    if (campus == "Multan")    return "NU-MUL-123";
    return "";
}

// ============================================================
//  Utility: get current timestamp string
// ============================================================
std::string timestamp() {
    time_t now = time(nullptr);
    struct tm t;
    localtime_s(&t, &now);
    char buf[20];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &t);
    return std::string(buf);
}

// ============================================================
//  Utility: split string by delimiter
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
//  Utility: print a banner line
// ============================================================
void banner(const std::string& text) {
    std::cout << "\n+==============================================+\n";
    std::cout <<   "|  " << text;
    // Pad to fixed width
    int pad = 44 - (int)text.size();
    for (int i = 0; i < pad; i++) std::cout << ' ';
    std::cout << "|\n";
    std::cout <<   "+==============================================+\n";
}

// ============================================================
//  TCP Receive Thread
//  Runs in background, continuously reads from server socket.
//  Handles:
//    MSG|...          - incoming campus message  -> stored in inbox
//    BROADCAST|...    - admin broadcast (also via UDP, but TCP fallback)
//    ERR|...          - error from server
//    AUTH_OK|...      - (handled in main before thread starts)
//    PONG|...         - response to TCP keep-alive ping
//    FILE|...         - incoming file transfer
//    ACK|...          - acknowledgement from server
// ============================================================
void receiveTCPThread() {
    char buffer[BUFFER_SIZE];

    while (running) {
        memset(buffer, 0, BUFFER_SIZE);
        int bytes = recv(tcpSocket, buffer, BUFFER_SIZE - 1, 0);

        if (bytes <= 0) {
            if (running) {
                std::cout << "\n[!] Connection to server lost. Please restart the client.\n";
                running = false;
            }
            break;
        }
        buffer[bytes] = '\0';
        std::string msg(buffer);

        // ---- MSG: incoming message from another campus ----
        if (msg.substr(0, 4) == "MSG|") {
            // Format: MSG|SrcCampus|SrcDept|DstCampus|DstDept|Text
            auto parts = split(msg, '|');
            if (parts.size() >= 6) {
                InboxMessage m;
                m.from     = parts[1];
                m.fromDept = parts[2];
                m.toDept   = parts[4];
                m.text     = parts[5];
                m.timeStr  = timestamp();

                {
                    std::lock_guard<std::mutex> lock(inboxMutex);
                    inbox.push_back(m);
                }

                // Notify user without disrupting their input
                std::cout << "\n\a"; // bell
                std::cout << "+---------------------------------------------+\n";
                std::cout << "|  NEW MESSAGE RECEIVED                        |\n";
                std::cout << "+---------------------------------------------+\n";
                std::cout << "|  From : " << m.from << " Campus - " << m.fromDept << "\n";
                std::cout << "|  To   : " << campusName << " - " << m.toDept << "\n";
                std::cout << "|  Time : " << m.timeStr << "\n";
                std::cout << "|  Msg  : " << m.text << "\n";
                std::cout << "+---------------------------------------------+\n";
            }

        // ---- ERR: server error notification ----
        } else if (msg.substr(0, 4) == "ERR|") {
            std::cout << "\n[SERVER ERROR] " << msg.substr(4) << "\n";

        // ---- ACK: file transfer acknowledgement ----
        } else if (msg.substr(0, 4) == "ACK|") {
            std::cout << "\n[ACK] " << msg.substr(4) << "\n";

        // ---- PONG: keep-alive response ----
        } else if (msg.substr(0, 5) == "PONG|") {
            // Silently ignore; just confirms TCP connection is alive

        // ---- FILE: incoming file from another campus ----
        } else if (msg.substr(0, 5) == "FILE|") {
            // Format: FILE|SrcCampus|DstCampus|Filename|FileSize\n<binary>
            // The buffer already contains header + some/all file bytes
            // Find the \n that separates text header from binary payload
            size_t nlPos = msg.find('\n');
            if (nlPos == std::string::npos) {
                std::cout << "\n[FILE ERROR] Malformed file packet (no header delimiter)\n";
                continue;
            }

            std::string header = msg.substr(0, nlPos);
            auto parts = split(header, '|');
            if (parts.size() < 5) continue;

            std::string srcCampus = parts[1];
            std::string filename  = parts[3];
            long        fileSize  = 0;
            try { fileSize = std::stol(parts[4]); } catch (...) { continue; }

            std::cout << "\n[FILE INCOMING] From " << srcCampus << ": "
                      << filename << " (" << fileSize << " bytes)\n";

            std::string savePath = "received_" + filename;
            std::ofstream ofs(savePath, std::ios::binary);
            if (!ofs.is_open()) {
                std::cout << "[FILE ERROR] Cannot create output file: " << savePath << "\n";
                continue;
            }

            // bytes is the raw recv length; nlPos+1 is where file data starts in buffer
            // bytes came from the recv call at top of loop which filled buffer[]
            long headerLen = (long)(nlPos + 1);
            long alreadyGot = (long)bytes - headerLen;
            if (alreadyGot > 0)
                ofs.write(buffer + headerLen, alreadyGot);

            // Read remaining file bytes
            long written = alreadyGot;
            char fileBuf[BUFFER_SIZE];
            while (written < fileSize) {
                int toRead = (int)std::min((long)BUFFER_SIZE, fileSize - written);
                int r = recv(tcpSocket, fileBuf, toRead, 0);
                if (r <= 0) break;
                ofs.write(fileBuf, r);
                written += r;
                std::cout << "\r[RECEIVING] " << written << "/" << fileSize << " bytes   " << std::flush;
            }
            ofs.close();
            std::cout << "\n[FILE SAVED] '" << savePath << "' ("
                      << written << " bytes written)\n";

        // ---- Other server messages ----
        } else {
            std::cout << "\n[SERVER] " << msg << "\n";
        }
    }
}

// ============================================================
//  Heartbeat Thread (UDP)
//  Sends a UDP datagram to the server every HEARTBEAT_INTERVAL
//  seconds to signal that this campus is online.
//  Format: HEARTBEAT|CampusName|online
// ============================================================
void heartbeatThread() {
    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port   = htons(UDP_PORT);
    inet_pton(AF_INET, SERVER_IP, &serverAddr.sin_addr);

    while (running) {
        std::string hb = "HEARTBEAT|" + campusName + "|online";
        sendto(udpSocket, hb.c_str(), (int)hb.size(), 0,
               (sockaddr*)&serverAddr, sizeof(serverAddr));
        // Uncomment for verbose heartbeat logging:
        // std::cout << "[HEARTBEAT SENT]\n";

        std::this_thread::sleep_for(std::chrono::seconds(HEARTBEAT_INTERVAL));
    }
}

// ============================================================
//  UDP Broadcast Receive Thread
//  Listens for admin broadcast announcements sent via UDP.
//  Format: BROADCAST|Message text here
// ============================================================
void receiveBroadcastThread() {
    char buffer[BUFFER_SIZE];
    sockaddr_in senderAddr{};
    int senderSize = sizeof(senderAddr);

    while (running) {
        memset(buffer, 0, BUFFER_SIZE);
        int bytes = recvfrom(udpSocket, buffer, BUFFER_SIZE - 1, 0,
                             (sockaddr*)&senderAddr, &senderSize);
        if (bytes <= 0) continue;

        buffer[bytes] = '\0';
        std::string msg(buffer);

        // Skip heartbeat echoes (shouldn't happen but guard anyway)
        if (msg.substr(0, 10) == "HEARTBEAT|") continue;

        if (msg.substr(0, 10) == "BROADCAST|") {
            std::cout << "\n\a";
            std::cout << "+==============================================+\n";
            std::cout << "|  *** SYSTEM-WIDE ANNOUNCEMENT ***             |\n";
            std::cout << "+==============================================+\n";
            std::cout << "|  " << msg.substr(10) << "\n";
            std::cout << "+==============================================+\n";
        }
    }
}

// ============================================================
//  Send a direct message to another campus via TCP
// ============================================================
void sendMessage() {
    std::string targetCampus, targetDept, myDept, message;

    std::cout << "\nYour department\n";
    std::cout << "  [1] Admissions  [2] Academics  [3] IT  [4] Sports\n";
    std::cout << "  Enter dept name (or number): ";
    std::getline(std::cin, myDept);

    // Accept numeric shortcut
    if (myDept == "1") myDept = "Admissions";
    else if (myDept == "2") myDept = "Academics";
    else if (myDept == "3") myDept = "IT";
    else if (myDept == "4") myDept = "Sports";

    std::cout << "Target campus (Islamabad/Lahore/Karachi/Peshawar/CFD/Multan): ";
    std::getline(std::cin, targetCampus);

    std::cout << "Target department (Admissions/Academics/IT/Sports): ";
    std::getline(std::cin, targetDept);

    std::cout << "Message: ";
    std::getline(std::cin, message);

    if (targetCampus.empty() || targetDept.empty() || message.empty()) {
        std::cout << "[!] All fields are required.\n";
        return;
    }

    // Build protocol message
    // Format: MSG|SrcCampus|SrcDept|DstCampus|DstDept|Text
    std::string msg = "MSG|" + campusName + "|" + myDept
                    + "|" + targetCampus + "|" + targetDept
                    + "|" + message;

    int sent = send(tcpSocket, msg.c_str(), (int)msg.size(), 0);
    if (sent == SOCKET_ERROR)
        std::cout << "[ERROR] Failed to send message (WSA: " << WSAGetLastError() << ")\n";
    else
        std::cout << "[OK] Message sent to " << targetCampus << " - " << targetDept << "\n";
}

// ============================================================
//  Send a file to another campus over TCP (Bonus Feature)
// ============================================================
void sendFile() {
    std::string targetCampus, filepath;

    std::cout << "\nTarget campus for file transfer: ";
    std::getline(std::cin, targetCampus);

    std::cout << "Path to file (e.g., report.txt): ";
    std::getline(std::cin, filepath);

    // Trim any trailing whitespace/CR from filepath
    filepath.erase(std::find_if(filepath.rbegin(), filepath.rend(),
        [](unsigned char c){ return !std::isspace(c); }).base(), filepath.end());

    // Open file in binary mode
    std::ifstream ifs(filepath, std::ios::binary | std::ios::ate);
    if (!ifs.is_open()) {
        std::cout << "[ERROR] Cannot open file: '" << filepath << "'\n";
        std::cout << "  Make sure the file exists in: C:\\Users\\PCS\\Desktop\\CN_PROJECT\\\n";
        return;
    }

    long fileSize = (long)ifs.tellg();
    ifs.seekg(0, std::ios::beg);

    // Read entire file into memory
    std::vector<char> fileData(fileSize);
    ifs.read(fileData.data(), fileSize);
    ifs.close();

    // Extract just the filename from the full path
    std::string filename = filepath;
    size_t slash = filepath.find_last_of("/\\");
    if (slash != std::string::npos)
        filename = filepath.substr(slash + 1);

    // Build packet: FILE|SrcCampus|DstCampus|Filename|FileSize\n<binary data>
    // The \n separates the text header from the binary payload
    std::string header = "FILE|" + campusName + "|" + targetCampus
                       + "|" + filename + "|" + std::to_string(fileSize) + "\n";

    // Combine header + file bytes into one buffer and send together
    std::vector<char> packet(header.begin(), header.end());
    packet.insert(packet.end(), fileData.begin(), fileData.end());

    std::cout << "[SENDING] " << filename << " (" << fileSize << " bytes) to " << targetCampus << "...\n";

    long totalSent = 0;
    long totalSize = (long)packet.size();
    const char* ptr = packet.data();

    while (totalSent < totalSize) {
        int chunk = (int)std::min((long)BUFFER_SIZE, totalSize - totalSent);
        int r = send(tcpSocket, ptr + totalSent, chunk, 0);
        if (r == SOCKET_ERROR) {
            std::cout << "[ERROR] File send interrupted (WSA: " << WSAGetLastError() << ")\n";
            return;
        }
        totalSent += r;
        // Show progress
        int pct = (int)((float)totalSent / totalSize * 100);
        std::cout << "\r[PROGRESS] " << pct << "% (" << totalSent << "/" << totalSize << " bytes)   " << std::flush;
    }

    std::cout << "\n[OK] File '" << filename << "' sent successfully to " << targetCampus << "\n";
}

// ============================================================
//  View Inbox: display all received messages
// ============================================================
void viewInbox() {
    std::lock_guard<std::mutex> lock(inboxMutex);

    if (inbox.empty()) {
        std::cout << "\n[Inbox is empty]\n";
        return;
    }

    std::cout << "\n+==============================================+\n";
    std::cout << "|         INBOX - " << campusName;
    int pad = 28 - (int)campusName.size();
    for (int i = 0; i < pad; i++) std::cout << ' ';
    std::cout << "|\n";
    std::cout << "+==============================================+\n";

    for (int i = 0; i < (int)inbox.size(); i++) {
        const auto& m = inbox[i];
        std::cout << "\n  [" << (i + 1) << "] " << m.timeStr << "\n";
        std::cout << "      From : " << m.from << " - " << m.fromDept << "\n";
        std::cout << "      To   : " << campusName << " - " << m.toDept << "\n";
        std::cout << "      Msg  : " << m.text << "\n";
        std::cout << "      -----------------------------------------\n";
    }
    std::cout << "\n  Total: " << inbox.size() << " message(s)\n";
}

// ============================================================
//  MAIN
// ============================================================
int main(int argc, char* argv[]) {
    // ---- Parse campus name from command line ----
    if (argc < 2) {
        std::cout << "Usage  : client.exe <CampusName>\n";
        std::cout << "Example: client.exe Lahore\n";
        std::cout << "Valid campuses: Islamabad, Lahore, Karachi, Peshawar, CFD, Multan\n";
        return 1;
    }
    campusName = argv[1];

    std::string password = getCampusPassword(campusName);
    if (password.empty()) {
        std::cout << "[ERROR] Unknown campus: '" << campusName << "'\n";
        std::cout << "Valid: Islamabad, Lahore, Karachi, Peshawar, CFD, Multan\n";
        return 1;
    }

    std::cout << "\n";
    std::cout << "+====================================================+\n";
    std::cout << "|     NU-Information Exchange System - CLIENT        |\n";
    std::cout << "|     Campus: " << campusName;
    int p = 39 - (int)campusName.size();
    for (int i = 0; i < p; i++) std::cout << ' ';
    std::cout << "|\n";
    std::cout << "+====================================================+\n\n";

    // ---- Initialize Winsock ----
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "[FATAL] Winsock initialization failed\n";
        return 1;
    }

    // ---- Create and Connect TCP Socket ----
    tcpSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (tcpSocket == INVALID_SOCKET) {
        std::cerr << "[FATAL] TCP socket creation failed\n";
        WSACleanup(); return 1;
    }

    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port   = htons(TCP_PORT);
    inet_pton(AF_INET, SERVER_IP, &serverAddr.sin_addr);

    std::cout << "[..] Connecting to Central Server at "
              << SERVER_IP << ":" << TCP_PORT << " ...\n";

    if (connect(tcpSocket, (sockaddr*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
        std::cerr << "[ERROR] Cannot connect to server. Is the server running?\n";
        closesocket(tcpSocket); WSACleanup(); return 1;
    }
    std::cout << "[OK] TCP connection established\n";

    // ---- Send AUTH Message ----
    std::string authMsg = "AUTH|Campus:" + campusName + ",Pass:" + password;
    send(tcpSocket, authMsg.c_str(), (int)authMsg.size(), 0);

    // ---- Wait for AUTH Response ----
    char buffer[BUFFER_SIZE];
    int bytes = recv(tcpSocket, buffer, BUFFER_SIZE - 1, 0);
    if (bytes <= 0) {
        std::cerr << "[ERROR] No response from server during auth\n";
        closesocket(tcpSocket); WSACleanup(); return 1;
    }
    buffer[bytes] = '\0';
    std::string authResp(buffer);

    if (authResp.substr(0, 7) != "AUTH_OK") {
        std::cout << "[ERROR] Authentication failed: " << authResp << "\n";
        closesocket(tcpSocket); WSACleanup(); return 1;
    }
    std::cout << "[OK] " << authResp.substr(8) << "\n\n"; // Print server welcome message

    // ---- Create UDP Socket ----
    udpSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (udpSocket == INVALID_SOCKET) {
        std::cerr << "[FATAL] UDP socket creation failed\n";
        closesocket(tcpSocket); WSACleanup(); return 1;
    }

    // Bind UDP socket to receive broadcast replies
    sockaddr_in udpAddr{};
    udpAddr.sin_family      = AF_INET;
    udpAddr.sin_port        = htons(0);         // OS assigns an ephemeral port
    udpAddr.sin_addr.s_addr = INADDR_ANY;
    bind(udpSocket, (sockaddr*)&udpAddr, sizeof(udpAddr));
    std::cout << "[OK] UDP socket ready for heartbeats and broadcasts\n";

    // ---- Start Background Threads ----
    std::thread(receiveTCPThread).detach();
    std::thread(heartbeatThread).detach();
    std::thread(receiveBroadcastThread).detach();

    std::cout << "[OK] Background threads started\n";
    std::cout << "     - TCP receive   : listening for messages\n";
    std::cout << "     - UDP heartbeat : every " << HEARTBEAT_INTERVAL << "s\n";
    std::cout << "     - UDP broadcast : listening for announcements\n\n";

    // ---- Main Console Menu ----
    while (running) {
        std::cout << "\n+--------------------------------------+\n";
        std::cout << "|  " << campusName << " Campus - Main Menu";
        int mp = 20 - (int)campusName.size();
        for (int i = 0; i < mp; i++) std::cout << ' ';
        std::cout << "|\n";
        std::cout << "+--------------------------------------+\n";
        std::cout << "|  1. Send Message to Another Campus    |\n";
        std::cout << "|  2. View Inbox (" << inbox.size() << " message(s))";
        int ip = 14 - (int)std::to_string(inbox.size()).size();
        for (int i = 0; i < ip; i++) std::cout << ' ';
        std::cout << "|\n";
        std::cout << "|  3. Send File (Bonus)                 |\n";
        std::cout << "|  0. Disconnect and Exit               |\n";
        std::cout << "+--------------------------------------+\n";
        std::cout << campusName << "> ";

        std::string line;
        if (!std::getline(std::cin, line)) break;
        if (line.empty()) continue;

        int choice = 0;
        try { choice = std::stoi(line); } catch (...) {
            std::cout << "[!] Invalid option. Enter 0-3.\n";
            continue;
        }

        switch (choice) {
            case 1:
                sendMessage();
                break;
            case 2:
                viewInbox();
                break;
            case 3:
                sendFile();
                break;
            case 0:
                running = false;
                std::cout << "\n[Disconnecting from NU Network...]\n";
                break;
            default:
                std::cout << "[!] Invalid option. Enter 0-3.\n";
        }
    }

    // ---- Graceful Shutdown ----
    closesocket(tcpSocket);
    closesocket(udpSocket);
    WSACleanup();
    std::cout << "[" << campusName << "] Client shut down. Goodbye.\n";
    return 0;
}