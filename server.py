import socket
import sys

def main():
    # Create UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    if len(sys.argv) != 2:
        print("Please input a port number in the cmd argument.")
        exit(-1)

    if int(sys.argv[1]) < 13100 or int(sys.argv[1]) > 13199:
        print("Please use a port number in the range of 13100-13199.")
        exit(-1)

    sock.bind(('127.0.0.1', int(sys.argv[1])))

    #Create Lists to Store 
    clients = {}
    disks = {}
    dss = {}

    #Handler Functions for register-user, register-disk, configure-dss
    def handle_register_user(split, addr):
        if len(split) != 5:
            return "FAILURE: Please make sure you have the correct arguments."
        
        username = split[1]
        ip = split[2]
        mport = split[3]
        cport = split[4]

        if username in clients:
            return "FAILURE: User already in the list."
        
        for values in clients.values():
            if values["m-port"] == mport or values["c-port"] == cport:
                return "FAILURE: Ports are already in use."
            
        clients[username] = {
            "ip": ip,
            "m-port": mport,
            "c-port": cport
        }
        return "SUCCESS"
    
    def handle_register_disk(split, addr):
        if len(split) != 5:
            return "FAILURE: Please make sure you have the correct arguments."
        
        diskname = split[1]
        ip = split[2]
        mport = split[3]
        cport = split[4]

        if diskname in clients:
            return "FAILURE: Disk already in the list."
        
        for values in disks.values():
            if values["m-port"] == mport or values["c-port"] == cport:
                return "FAILURE: Ports are already in use."
            
        disks[diskname] = {
            "ip": ip,
            "m-port": mport,
            "c-port": cport
        }
        return "SUCCESS"
    
    def hand_configure_dss(split, addr):
        return


    # Data 
    while True:
        data, addr = sock.recvfrom(1024)

        #Allocate the message
        message = data.decode("utf-8").strip()

        split = message.split()

        if not split:
            continue
        command = split[0]

        #Handle Commands
        if command == "register-user":
            handler = handle_register_user(split, addr)
        elif command == "register-disk":
            handler = handle_register_disk(split, addr)
        elif command == "configure-dss":
            handler = handler_configure_dss(split, addr)
        else:
            handler = "Invalid command. Please type register-user, register-disk, or configure-dss."

main()

