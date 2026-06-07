# LAN File Transfer Assistant (lan-share) Design Specification

This document details the design and specification for `lan-share`, a command-line interface (CLI) tool for local area network (LAN) service discovery, text message transmission, and file sharing. It is built in Rust and runs cross-platform on macOS, Windows, and Linux.

## 1. System Architecture

`lan-share` compiles into a single executable binary that can run either in **Server** mode (acting as a background or foreground daemon) or **Client** mode (to execute commands, send files, list peers, etc.).

```
                         +--------------------------+
                         |        lan-share         |
                         +--------------------------+
                                      |
                 +--------------------+--------------------+
                 |                                         |
     +-----------v-----------+                 +-----------v-----------+
     |  HTTP Server (Axum)   |                 |      CLI / Client     |
     +-----------+-----------+                 +-----------+-----------+
                 |                                         |
    [POST /api/message, /api/file]            [Send text/files via Reqwest]
                 |                                         |
                 +--------------------+--------------------+
                                      |
                         +------------v------------+
                         |    Discovery Engine     |
                         |  (UDP Multicast Loop)   |
                         +-------------------------+
```

### Components
1.  **Service Discovery Engine**:
    *   **UDP Multicast Broadcaster**: Regularly broadcasts heartbeats onto the local subnet.
    *   **UDP Multicast Listener**: Listens for heartbeats from other active `lan-share` instances and maintains an in-memory registry of active peers.
2.  **HTTP Server (Axum & Tokio)**:
    *   Listens on a configured or auto-allocated port (default `8080`).
    *   Exposes endpoints to receive text payloads and files (via streaming multipart).
3.  **HTTP Client (Reqwest)**:
    *   Issues requests to remote instances when sending text messages or files.
4.  **CLI Interface (Clap)**:
    *   Parses subcommands (`serve`, `peers`, `send-text`, `send-file`).

---

## 2. Protocols and APIs

### 2.1 Service Discovery (UDP Multicast)
*   **Multicast IPv4 Address**: `224.0.0.188`
*   **Port**: `50001`
*   **Interval**: 3 seconds (Heartbeat timeout is set to 9 seconds)
*   **Payload Format (JSON)**:
    ```json
    {
      "uuid": "String (UUID v4)",
      "name": "String (Node hostname or custom name)",
      "port": 8080,
      "ips": ["String (List of IPv4 addresses)"]
    }
    ```

### 2.2 HTTP APIs

#### 2.2.1 Receive Text Message
*   **Endpoint**: `POST /api/message`
*   **Content-Type**: `application/json`
*   **Payload (JSON)**:
    ```json
    {
      "sender_name": "String",
      "text": "String"
    }
    ```
*   **Response**: `200 OK` with `{"status": "success"}`.

#### 2.2.2 Receive File
*   **Endpoint**: `POST /api/file`
*   **Content-Type**: `multipart/form-data`
*   **Fields**:
    *   `file`: The binary payload (multipart file upload).
    *   `sender_name`: Hostname/name of the sender.
*   **Response**: `200 OK` with `{"status": "success"}`.
*   **Action**: Saves the file automatically to the configured download folder (default: `./downloads/`) using its original filename.

---

## 3. CLI Design

### 3.1 Subcommands

1.  `serve`
    *   Starts the HTTP server and UDP service discovery loop.
    *   Flags:
        *   `--port <PORT>`: Port to listen on (default: `8080`, increments if busy).
        *   `--name <NAME>`: Custom name for this node (default: system hostname).
        *   `--dir <DIR>`: Directory where incoming files are saved (default: `./downloads/`).
2.  `peers`
    *   Interrogates the local UDP registry and lists currently active nodes.
3.  `send-text`
    *   Sends a text message to a specific peer.
    *   Flags:
        *   `--to <IP:PORT | NODE_NAME>`: Target identifier.
        *   `--msg <TEXT>`: Message content.
4.  `send-file`
    *   Sends a file to a specific peer.
    *   Flags:
        *   `--to <IP:PORT | NODE_NAME>`: Target identifier.
        *   `--file <PATH>`: Path to local file to be sent.

---

## 4. Error Handling and Edge Cases
1.  **Port Collisions**: If port `8080` is in use when starting `serve`, the application will attempt to bind to `8081`, `8082`, etc., and broadcast the successful port in its heartbeats.
2.  **Multiple Interfaces / IPs**: The heartbeat payload contains all IPv4 addresses of the host. The client filters out loopbacks and private subnet mismatches to pick the appropriate destination IP.
3.  **File Naming Collisions**: If a file with the same name already exists in the target directory, a numeric suffix (e.g. `file_1.txt`) is appended to prevent overwriting.
