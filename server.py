import socket
import ipaddress
import sys

def handle_register_user(split, data):
    # Syntax check
    if len(split) != 5:
        return "FAILURE: Please make sure you have the correct arguments."
    
    username = split[1]
    ip = split[2]
    mport = split[3]
    cport = split[4]
    state = "Free"
    
    if username in data:
        return "FAILURE: User already in the list."
    
    try:
        ipaddress.ip_address(ip)
    except ValueError:
        return "FAILURE: Invalid IP address for register-user-command."

    for values in data.values():
        if values["m-port"] == mport or values["c-port"] == cport:
            return "FAILURE: Ports are already in use."
        
    data[username] = {
        "ip": ip,
        "m-port": mport,
        "c-port": cport,
        "state": state
    }
    return "SUCCESS"

def handle_register_disk(split, data):
    # Syntax check
    if len(split) != 5:
        return "FAILURE: Please make sure you have the correct arguments."
    
    diskname = split[1]
    ip = split[2]
    mport = split[3]
    cport = split[4]

    if diskname in data:
        return "FAILURE: Disk already in the list."
    
    try:
        ipaddress.ip_address(ip)
    except ValueError:
        return "FAILURE: Invalid IP address for register-disk-command."

    for values in data.values():
        if values["m-port"] == mport or values["c-port"] == cport:
            return "FAILURE: Ports are already in use."
        
    data[diskname] = {
        "ip": ip,
        "m-port": mport,
        "c-port": cport
    }
    return "SUCCESS"

# TODO: Implement the handler for configuring dss after implementing client and disk to have both ports.
def handle_configure_dss(split, addr):
    return

def deregister_user(username, data):
    if username in data:
        del data[username]
        return "SUCCESS"
    else:
        return "FAILURE"

# TODO Add check for disks in-use.  
def deregister_disk(diskname, data):
    if diskname in data:
        del data[diskname]
        return "SUCCESS"
    else:
        return "FAILURE"

def main():
    # Syntax Check
    if len(sys.argv) != 2:
        print("Please make sure you have the correct arguments.")
        print("server.py <server_port>")
        exit(-1)

    if int(sys.argv[1]) < 13100 or int(sys.argv[1]) > 13199:
        print("Please use a port number in the range of 13100-13199.")
        exit(-1)

    # Create UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Bind Socket
    sock.bind(('127.0.0.1', int(sys.argv[1])))

    print("Server is now online !")

    # Dictionaries and ints to store data
    clients = {}
    disks = {}
    dss = {}
    reg_user_req_count = 0
    reg_disk_req_count = 0
    con_dss_req_count  = 0
    del_user_req_count = 0
    del_disk_req_count = 0

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
            reg_user_req_count += 1
            handler = handle_register_user(split, clients)
            response = handler.encode('utf-8')
            print(f"Register-user command request number: {reg_user_req_count}")
            print(f"Users registered is now: {clients}")
            sock.sendto(response, addr)

        elif command == "register-disk":
            reg_disk_req_count += 1
            handler = handle_register_disk(split, disks)
            response = handler.encode('utf-8')
            print(f"Register-disk command request number: {reg_disk_req_count}")
            print(f"Disks registered is now: {disks}")
            sock.sendto(response, addr)

        elif command == "configure-dss":
            con_dss_req_count += 1
            handler = handle_configure_dss(split, addr)
            print(f"Configure-dss command request number: {con_dss_req_count}")
            response = handler.encode('utf-8')
            sock.sendto(response, addr)

        elif command == "deregister-user":
            del_user_req_count += 1
            username = split[1]
            handler = deregister_user(username, clients)
            print(f"Delete-user command request number: {del_user_req_count}")
            response = handler.encode('utf-8')
            sock.sendto(response, addr)

        elif command == "deregister-user":
            del_disk_req_count += 1
            diskname = split[1]
            handler = deregister_disk(diskname, data)
            print(f"Delete-disk command request number: {del_disk_req_count}")
            response = handler.encode('utf-8')
            sock.sendto(response, addr)

        else:
            handler = "Invalid command. Commands available: register-user, register-disk, or configure-dss."

main()


