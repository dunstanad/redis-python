import socket
import threading
from datetime import datetime,timedelta 
import argparse

# eg. parts = ['*5', '$3', 'set','$5','fruit' ,'$5', 'pears', '$2', 'px', '$3', '100']  
# *5 : means there are 5 values sent from client side i.e   set fruit pears px 100

dictionary = dict()  # store all the key value pairs
 
serverInfo = {  # store info about server   eg. portnum: role  6379:master
        "role": "master",
        "master_replid": "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
        "master_repl_offset": 0
        
}

def checkIfExpired(key):  
    if datetime.now() <  dictionary[key]['expiration']  :
        return False # key is not expired
    else:
        del dictionary[key] # delete key because it's expired
        return True  # key is expired


def setKeyExpiry(key, value, microsecs):
    current = datetime.now()
    print(current)
    time_delta = timedelta(microseconds= microsecs)
    expire = current  + time_delta
    print(expire)
    dictionary[key] = {'value': value, 'expiration': expire}
    print(dictionary)




def checkPX(pxvalue):   # function checks if there is px
    print("WE ARE IN checkPX")
    return pxvalue.lower() == 'px'


def hasExpiry(key):  # function to check if key has expiration as value
    return 'expiration' in dictionary[key]

def commandSET(parts):
    elementsPassed = int(parts[0][1:])    # fetch value *3 -> 3    ['*3', '$3', 'set','$5','fruit' ,'$5', 'pears']
    # elementsPassed gives no. of elements passed:  set foo bar px 100
    key = parts[4] 
    value = parts[6]
  
    if elementsPassed > 3:   # check if more than 3 elements:   set foo bar px 100
        if checkPX(parts[8]):  # check if px exists  
            print("Going to set expiry")
            microsecs = int(parts[10]) * 1000    # int(parts[10]) is  milliseconds value, later converted to microseconds
            print(microsecs)
            setKeyExpiry(key, value, microsecs)
            
    else:
        dictionary[key]  = value  # eg. parts = ['*3', '$3', 'set','$5','fruit' ,'$5', 'pears']
        print("Key: "+key+"  Value: "+value)

    return "+OK\r\n"   # send OK as response to set command

def commandGET(parts):
    key = parts[4]    # eg. parts = ['*2', '$3', 'get','$5','fruit' ]

    if key in dictionary and hasExpiry(key):  #check if key is present and has expiration
        if not checkIfExpired(key):  # check if the key is expired
            value = dictionary[key]['value']        # fetching value
            return f"${len(value)}\r\n{value}\r\n"    # return bulk string with value

    elif key in dictionary and not hasExpiry(key): #check if key is present and has no expiration
        value = dictionary[key]        # fetching value
        return f"${len(value)}\r\n{value}\r\n"    # return bulk string with value

    return "$-1\r\n"   #return null bulk string if no key is present or expired



def bulkString(parts, isMaster):
     # eg. parts = ['*5', '$3', 'set','$5','fruit' ,'$5', 'pears', '$2', 'px', '$3', '100']
     # eg. parts = ['*3', '$3', 'set','$5','fruit' ,'$5', 'pears']

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
        return commandSET(parts)
   
    elif command == 'get': 
        return commandGET(parts)       

    elif command == 'info':
        if isMaster:
                    role = serverInfo['role']
                    master_replID = serverInfo['master_replid']
                    master_replOFFSET = serverInfo['master_repl_offset']
                    role_str = "role:" + role
                    replid_str = "master_replid:" + master_replID
                    offset_str = "master_repl_offset:" + str(master_replOFFSET)
                    print(role)
                    return f"${len(role_str)}\r\n{role_str}\r\n${len(replid_str)}\r\n{replid_str}\r\n${len(offset_str)}\r\n{offset_str}\r\n"
                    #return f"$11\r\nrole:{role} \r\n"
        else:
            print(serverInfo)
            return f"$10\r\nrole:slave\r\n"




def handleConnections(conn, isMaster):
    try:
        with conn:
            while True:
                data = conn.recv(1024).decode()
                if not data:
                    break
                print("Data "+repr(data))  # this will print something like *1\r\n$4\r\nping\r\n   or  *2\r\n$4\r\necho\r\n$5\r\npears\r\n  
                parts = data.strip().split("\r\n")  # ['*1', '$4', 'ping']   ['*2', '$4', 'echo', '$5', 'pears']
                print(parts)
                s = bulkString(parts, isMaster)  # final string   $4\r\nPONG\r\n  or  $5\r\npears\r\n
                print("Response ",s)
                conn.send(s.encode())   # encoding the bulk string as response
       
    except Exception as e:
        print("The ERROR is ",e)

def main():
    pong = "+PONG\r\n"
    portNumber = None
    isMaster = True
    parser = argparse.ArgumentParser()  # parse the arguments
    parser.add_argument("--port", type= int, help="used to set to port number to listen to requests")
    parser.add_argument("--replicaof", nargs=2, metavar=('masterhost', 'masterport'), help="Replicate to another Redis instance")
    args = parser.parse_args()

    if args.port:  #slave
        portNumber = args.port
        isMaster = False
        print("SLAVE :",portNumber)   
    elif not args.port: # master
        portNumber = 6379  # default port number
        #server_socket = socket.create_server(("localhost", portNumber), reuse_port=True)
    
    if args.replicaof:
        isMaster = False 

    #server_socket = socket.create_server(("localhost", portNumber), reuse_port=True) 

    try:
        while True:     
            server_socket = socket.create_server(("localhost", portNumber), reuse_port=True)
            conn, addr = server_socket.accept() # wait for client
            threading.Thread(target=handleConnections, args=(conn, isMaster)).start()
    except Exception as e:
        print("The ERROR is ",e)

if __name__ == "__main__":
    main()
