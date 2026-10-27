import threading
import socket

connections = {}
client_count = 0
server_count = 0

HOST = "127.0.0.1"
PORT = 5555
bridge = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
bridge.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
bridge.bind((HOST, PORT))
bridge.listen()
print(f"Server is listening on: {HOST}, {PORT}")

def sendMessage(conn, target):
    while True:
        cmd = conn.recv(1029).decode().strip()

        if not cmd:
            continue

        target.sendall(cmd.encode())

        if cmd == "exit":
            conn.sendall(b"Disconnected from client")
            return

        reply = target.recv(1029).decode().strip()
        conn.sendall(reply.encode())

def askOpt(conn):
    clients = [name for name in connections if name.startswith("client")]
    if not clients:
        conn.sendall(b"No targets connected")
        return
    menu = "\n".join(
f"{c}|{targets}" for c, targets in enumerate(clients, start=1)
)
    conn.sendall(menu.encode())
    while True:
        data = conn.recv(1029).decode().strip()
        if not data:
            break
        if data == "exit":
            return
        if data == "list":
            conn.sendall(b"Please select a target number, or type exit")
            continue
        index = int(data) - 1
        if index < 0 or index >= len(clients):
            conn.sendall(b"invalid selection")
            continue
        name = clients[index]
        if name not in connections:
            conn.sendall(b"Target disconnected. Refresh the list.")
            return
        target = connections[name]
        conn.sendall(f"Selected target {data}".encode())
        sendMessage(conn, target)
        return

def handle_client(conn):
    global client_count
    global server_count
    try:
        while True:
            data = conn.recv(1029).decode().strip().lower()
            if not data:
                break

            if data == "client":
                client_count += 1 #adds an id to the new device
                identity = data + str(client_count)
                connections[identity] = conn

            if data == "server":
                server_count += 1
                identity = data + str(server_count)
                connections[identity] = conn

            if data == "list":
                askOpt(conn)
            print(data)
    except Exception as e:
            print(f"Error type: {type(e).__name__}")
            raise
    finally:
        for identity, sock in list(connections.items()):
            if sock is conn:
                del connections[identity]
                break
        conn.close()

def handle_connection():
    global client_count
    while True:
        try:
            conn,addr = bridge.accept()
            c = threading.Thread(target=handle_client, args=(conn,), daemon=True)
            c.start()
            print(f"Connected: {addr}")
        except Exception as e:
            print(f"An error occoured: {e}")

t = threading.Thread(target=handle_connection, daemon=False)
t.start()

