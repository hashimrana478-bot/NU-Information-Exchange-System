# NU-Information Exchange System
### FAST-NUCES Multi-Campus Network | Computer Networks Semester Project — Fall 2025

---

## Overview
A fully functional inter-campus communication platform connecting FAST-NUCES campuses across Pakistan. Built in C++ using Windows Sockets 2 (Winsock) API, implementing a hybrid TCP/UDP architecture with a custom application-layer protocol.

---

## Features
- **TCP Multi-threaded Server** — one thread per campus, handles up to 10 simultaneous connections
- **Campus Authentication** — credential-based login before any communication is allowed
- **Inter-Campus Messaging** — route messages between any two campuses and departments
- **UDP Heartbeat Monitoring** — each campus sends status every 10 seconds
- **Admin Dashboard** — real-time view of all connected campuses with last-seen timestamps
- **System-Wide Broadcast** — admin can send announcements to all campuses via UDP
- **Inbox** — received messages stored and viewable per session
- **Bonus: File Transfer** — send files between campuses over TCP

---

## Campuses Supported
| Campus | Credential |
|--------|-----------|
| Islamabad | NU-ISB-123 |
| Lahore | NU-LHR-123 |
| Karachi | NU-KHI-123 |
| Peshawar | NU-PSH-123 |
| CFD | NU-CFD-123 |
| Multan | NU-MUL-123 |

---

## Compile
```bash
g++ server.cpp -o server.exe -lws2_32 -std=c++17
g++ client.cpp -o client.exe -lws2_32 -std=c++17
```

## Run
```bash
# Terminal 1 — Start Server
.\server.exe

# Terminal 2, 3, 4 — Start Campus Clients
.\client.exe Lahore
.\client.exe Karachi
.\client.exe Multan
```

---

## Protocol
| Message | Format |
|---------|--------|
| Auth | `AUTH\|Campus:Lahore,Pass:NU-LHR-123` |
| Message | `MSG\|SrcCampus\|SrcDept\|DstCampus\|DstDept\|Text` |
| Heartbeat | `HEARTBEAT\|CampusName\|online` |
| Broadcast | `BROADCAST\|AnnouncementText` |
| File | `FILE\|Src\|Dst\|Filename\|Size\n<bytes>` |

---

## Tech Stack
- **Language:** C++17
- **API:** Windows Sockets 2 (Winsock)
- **Concurrency:** std::thread, std::mutex
- **Protocols:** TCP (port 9000) + UDP (port 9001)

---

## Department: Computer Science | FAST-NUCES | Fall 2025
