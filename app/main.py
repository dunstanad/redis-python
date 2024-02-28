import socket
import threading

dictionary = dict()  # store all the key value pairs
def bulkString(parts):
    command = parts[2].lower()

    if command == 'ping':  # if data contains 'ping'
        return f"$4\r\nPONG\r\n"
    elif command == 'echo':     # if data contains 'echo some_argument'
        s = ""
        for i in range(3,len(parts)):
            s += parts[i]
            s += f"\r\n"
        return s
    elif command == 'set':
        dictionary[parts[4]]  = parts[5]  # eg. parts = ['*2', '$3', 'set','$5','fruit' ,'$5', 'pears']
        print("Key: "+parts[4]+"  Value: "+parts[5])
        return "+OK\r\n"   # send OK as response to set command


def handleConnections(conn):
    with conn:
        while True:
            data = conn.recv(1024).decode()
            if not data:
                break
            print("Data "+repr(data))  # this will print something like *1\r\n$4\r\nping\r\n   or  *2\r\n$4\r\necho\r\n$5\r\npears\r\n  
            parts = data.strip().split("\r\n")  # ['*1', '$4', 'ping']   ['*2', '$4', 'echo', '$5', 'pears']
            print(parts)
            s = bulkString(parts)  # final string   $4\r\nPONG\r\n  or  $5\r\npears\r\n
            conn.send(s.encode())   # encoding the bulk string as response

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
