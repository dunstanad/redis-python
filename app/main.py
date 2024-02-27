# Uncomment this to pass the first stage
import socket
import threading


def handleConnections(conn, pong):
    with conn:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            conn.send(pong.encode())


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    pong = "+PONG\r\n"
    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
   # conn, addr = server_socket.accept() # wait for client

    
    while True:
        conn, addr = server_socket.accept() # wait for client
        threading.Thread(target=handleConnections, args=(conn,pong)).start()

if __name__ == "__main__":
    main()
