import socket
import ipaddress
import sys
import random

def handle_register_user(split, data):
    # Syntax check
    if len(split) != 5:
        return "FAILURE: Please make sure you have the correct arguments."
    
    username = split[1]
    ip = split[2]
    mport = split[3]
    cport = split[4]

    if len(username) > 15:
        return "FAILURE: User name has to be 15 characters or less."

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
        "c-port": cport
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
    state = "Free"

    if len(diskname) > 15:
        return "FAILURE: Disk name has to be 15 characters or less."

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
        "c-port": cport,
        "state": state
    }
    return "SUCCESS"

def handle_configure_dss(split, data_dss, data_disk):
    # Syntax check
    if len(split) != 4:
        return "FAILURE: Please make sure you have the correct arguments."
    
    dssname = split[1]
    #Validate the inputs
    try: 
        number_of_disk = int(split[2])
        striping_unit = int(split[3])

    except ValueError:
        return "FAILURE: Disks and striping unit must be integers."
    
    if number_of_disk < 3:
        return "FAILURE: Make sure the number of disks for the DSS is greater than or equal to 3."
    
    if sum(1 for disk in data_disk.values() if disk['state'] == "Free") < number_of_disk:
        return "FAILURE: Insufficient amount of free disks for DSS."

    if not (striping_unit == 128 or striping_unit == 256 or striping_unit == 512 or striping_unit == 1024):
        return "FAILURE: Make sure the striping unit is beteen 128 and 1024 bytes."
    
    if dssname in data_dss:
        return "FAILURE: Dss name already in use."
    
    # Initialize dss entry to have disks, striping unit, and files.
    data_dss[dssname] = {
        "disks": [],        #Have disknames as a list
        "striping_unit": striping_unit,
        "files": []
    }

    # add to data
    for i in range(number_of_disk):
        for disk_name, disk in data_disk.items():
            if disk['state'] == "Free":
                data_dss[dssname]["disks"].append(disk_name)
                disk['state'] = "InDSS"
                break    

    return "SUCCESS"

def deregister_user(username, data):
    if username in data:
        del data[username]
        return "SUCCESS"
    else:
        return "FAILURE: User not in system."
  
def deregister_disk(diskname, data):
    if diskname in data:
        if data[diskname]['state'] == "InDSS":
            return "FAILURE: Disk in use."
        else:
            del data[diskname]
            return "SUCCESS"     
    else:
        return "FAILURE: Disk not in system."
    
def handle_ls(dss):
    if not dss:
        return "FAILURE: There is no DSS configured."
    
    Success = ["SUCCESS"]   # Indicate success line
    
    #We need to iterate each DSS
    for dssname, dss_info in dss.items():
        disks = ", ".join(dss_info["disks"])
        striping_unit = dss_info["striping_unit"]
        n = len(dss_info["disks"])
        Success.append(f"{dssname}: Disk array with n={n} ({disks}) with striping-unit {striping_unit} B")
        
        #List every file stored on the DSS
        for file_info in dss_info['files']:
            filename = file_info['filename']
            size = file_info['filesize']
            owner = file_info['owner']
            Success.append(f"{filename} {size} B {owner}")
            
    return "\n".join(Success)

def handle_copy(split, dss, data, username):
    # Phase 1:

    filename = split[1]
    filesize = int(split[2])
    owner = split[3]

    if not dss:
        return "FAILURE: No DSS configured."
    
    # Select a random dss.
    dss_name = random.choice(list(dss.keys()))
    dss_info = dss[dss_name]

    num_drives = len(dss_info['disks'])
    striping_unit = dss_info['striping_unit']

    # Build the response.
    response = f"{dss_name} {num_drives} {striping_unit}"
    for disk_name in dss_info['disks']:
        disk_data = data[disk_name]
        response += f" {disk_name} {disk_data['ip']} {disk_data['c-port']}"

    # Create a pending copy, will use later to actually put the entry in phase 2.
    if 'pending_copy' not in dss_info:
        dss_info['pending_copy'] = {}

    dss_info['pending_copy']['filename'] = filename
    dss_info['pending_copy']['filesize'] = filesize
    dss_info['pending_copy']['owner'] = owner

    return response

def handle_copy_complete(dss, dss_name):
    # Phase 2:
    dss_info = dss[dss_name]

    if 'pending_copy' not in dss_info:
        return "FAILURE"
    
    pending = dss_info['pending_copy']

    file_entry = {
        "filename": pending['filename'],
        "filesize": pending['filesize'],
        "owner": pending['owner']
    }    
    dss_info['files'].append(file_entry)

    # Get rid of the pending copy as it is now added onto the dss.
    del dss_info['pending_copy']

    print()
    return "SUCCESS"

def handle_read(split, addr):
    print()

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
    sock.bind(('', int(sys.argv[1])))

    print("Server is now online !")

    # Dictionaries and ints to store data
    clients = {}
    disks = {}
    dss = {}
    reg_user_req_count = 0, reg_disk_req_count = 0, con_dss_req_count  = 0
    del_user_req_count = 0, del_disk_req_count = 0
    ls_req_count = 0, copy_req_count = 0, read_req_count = 0

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
            handler = handle_configure_dss(split, dss, disks)
            print(f"Configure-dss command request number: {con_dss_req_count}")
            print(f"Configured dss's is now: {dss}")
            response = handler.encode('utf-8')
            sock.sendto(response, addr)

        elif command == "deregister-user":
            del_user_req_count += 1
            username = split[1]
            handler = deregister_user(username, clients)
            print(f"Delete-user command request number: {del_user_req_count}")
            response = handler.encode('utf-8')
            sock.sendto(response, addr)

        elif command == "deregister-disk":
            del_disk_req_count += 1
            diskname = split[1]
            handler = deregister_disk(diskname, disks)
            print(f"Delete-disk command request number: {del_disk_req_count}")
            response = handler.encode('utf-8')
            sock.sendto(response, addr)
            
        elif command == "ls":
            ls_req_count += 1
            handler = handle_ls(dss)
            response = handler.encode('utf-8')
            print(f"LS command request number: {ls_req_count}")
            print(f"Current DSS List: {dss}")
            sock.sendto(response, addr)

        elif command == "copy":
            copy_req_count += 1
            username = split[3]
            handler = handle_copy(split, dss, disks, username)
            response = handler.encode('utf-8')
            print(f"Copy command request number: {copy_req_count}")
            sock.sendto(response, addr)

            # Skip the rest if there is a failure response.
            if response == b"FAILURE: No DSS configured.":
                continue

            dss_name = handler.split()[0]

            # Wait for copy-complete
            data, addr = sock.recvfrom(1024)
            message = data.decode("utf-8").strip()

            # Handle client response.
            if message == "copy-complete":

                response = handle_copy_complete(dss, dss_name)
                sock.sendto(response.encode('utf-8'), addr)
            else:
                sock.sendto(b"FAILURE", addr)

            print(f"DSS '{dss_name}' is now {dss[dss_name]}.")

        elif command == "read":
            read_req_count += 1
            handler = handle_read(split, addr)
            response = handler.encode('utf-8')

            sock.sendto(response, addr)

        elif command == "print":
            print(f"Disks registered is now: {disks}")

        else:
            handler = "Invalid command. Commands available: register-user, register-disk, configure-dss, deregister-user, deregister-disk."
            response = handler.encode('utf-8')
            sock.sendto(response, addr)

main()
