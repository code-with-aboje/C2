import os
import threading
import socket

connections = {}
client_count = 0
server_count = 0

HOST = "0.0.0.0"  # Changed from 127.0.0.1 so Railway binds correctly to external traffic
PORT = int(os.environ.get("PORT", 5555))

bridge = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
bridge.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
bridge.bind((HOST, PORT))
bridge.listen()
print(f"Server is listening on: {HOST}, {PORT}")

def forward_stream(source, destination, on_disconnect):
    """Safely pipes raw data from one socket directly to another without stripping newlines."""
    try:
        while True:
            data = source.recv(4096)
            if not data:
                break
            
            # If server sends 'exit', forward it and terminate session
            if b"exit" in data.lower():
                try:
                    destination.sendall(data)
                except:
                    pass
                break
                
            destination.sendall(data)
    except Exception:
        pass
    finally:
        on_disconnect()

def sendMessage(conn, target):
    """Launches parallel threads to pipe Server <-> Client simultaneously without blocking."""
    disconnect_event = threading.Event()
    
    def handle_disconnect():
        disconnect_event.set()

    # Thread 1: Server -> Client
    server_to_client = threading.Thread(
        target=forward_stream, 
        args=(conn, target, handle_disconnect), 
        daemon=True
    )
    
    # Thread 2: Client -> Server
    client_to_server = threading.Thread(
        target=forward_stream, 
        args=(target, conn, handle_disconnect), 
        daemon=True
    )

    server_to_client.start()
    client_to_server.start()

    # Hold this function scope open until a connection closes
    disconnect_event.wait()
    print("Session closed between server and target.")

def askOpt(conn):
    clients = [name for name in connections if name.startswith("client")]
    if not clients:
        conn.sendall(b"No targets connected\n")
        return
    
    menu = "\n".join(f"{c}|{targets}" for c, targets in enumerate(clients, start=1)) + "\n"
    conn.sendall(menu.encode())
    
    while True:
        data = conn.recv(1029).decode().strip()
        if not data:
            break
        if data == "exit":
            return
        if data == "list":
            conn.sendall(b"Please select a target number, or type exit\n")
            continue
        
        try:
            index = int(data) - 1
            if index < 0 or index >= len(clients):
                conn.sendall(b"invalid selection\n")
                continue
            
            name = clients[index]
            if name not in connections:
                conn.sendall(b"Target disconnected. Refresh the list.\n")
                return
                
            target = connections[name]
            conn.sendall(f"Selected target {data}\n".encode())
            sendMessage(conn, target)
            return
        except ValueError:
            conn.sendall(b"invalid selection, please enter a number\n")
            continue

def handle_client(conn):
    global client_count
    global server_count
    try:
        while True:
            # Use raw lookahead or split lines for configuration handshakes
            data = conn.recv(1029).decode().strip().lower()
            if not data:
                break

            if data == "client":
                client_count += 1
                identity = "client" + str(client_count)
                connections[identity] = conn
                print(f"Registered: {identity}")
                return  # Break loop to preserve this socket for streaming context

            elif data == "server":
                server_count += 1
                identity = "server" + str(server_count)
                connections[identity] = conn
                print(f"Registered: {identity}")
                
                # Stay inside server loop to handle structural menus
                while True:
                    server_data = conn.recv(1029).decode().strip().lower()
                    if not server_data:
                        break
                    if server_data == "list":
                        askOpt(conn)
                break
    except Exception as e:
        print(f"Error handling connection: {e}")
    finally:
        for identity, sock in list(connections.items()):
            if sock is conn:
                del connections[identity]
                print(f"Disconnected: {identity}")
                break
        try:
            conn.close()
        except:
            pass

def handle_connection():
    while True:
        try:
            conn, addr = bridge.accept()
            c = threading.Thread(target=handle_client, args=(conn,), daemon=True)
            c.start()
            print(f"Connected incoming: {addr}")
        except Exception as e:
            print(f"An error occurred in accept loop: {e}")

t = threading.Thread(target=handle_connection, daemon=False)
t.start()
