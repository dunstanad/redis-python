import socket
import threading


# def handleConnections(conn, pong):
#     with conn:
#         while True:
#             data = conn.recv(1024)
#             if not data:
#                 break
#             conn.send(pong.encode())
# def bulkRESP(parts):
#     if parts[2].lower() == 'ping':
#         return r"$4\r\nPONG\r\n"
#     else:


#     for i in range(1,len(parts)-1):
    


#     pass

def handleConnections(conn):
    with conn:
        while True:
            data = conn.recv(1024).decode()
            if not data:
                break
            print("Data "+repr(data))  # this will print something like *1\r\n$4\r\nping\r\n   or  *2\r\n$4\r\necho\r\n$5\r\npears\r\n  
            parts = data.split("\r\n")  # ['*1', '$4', 'ping', '']   ['*2', '$4', 'echo', '$5', 'pears', '']
            print(parts)
            #bulkRESP(parts)
            



            conn.send(pong.encode())

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    pong = "+PONG\r\n"
    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    
    while True:
        conn, addr = server_socket.accept() # wait for client
        threading.Thread(target=handleConnections, args=(conn,)).start()

if __name__ == "__main__":
    main()
