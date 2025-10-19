import socket
import ipaddress
import sys
import os
import threading
import math

def copy_file_to_dss(filename, filesize, disks, dss_name, num_drives, striping_unit, sock_peer):
    # Copy a file to DSS using block-interleaved distributed parity.
    print(f"\nCopying {filename} ({filesize} bytes) to {dss_name}.")
    print(f"DSS parameters: number of drives = {num_drives}, striping unit = {striping_unit} bytes.")

    # Calculate amount of stripes required for file.
    data_blocks_per_stripe = num_drives - 1
    bytes_per_stripe = data_blocks_per_stripe * striping_unit
    num_stripes = math.ceil(filesize/bytes_per_stripe)
 
    print(f"\n-- STRIPING CALCULATIONS for '{filename}' --")
    print(f"Data blocks per stripe: {data_blocks_per_stripe}.")
    print(f"Bytes per stripe: {bytes_per_stripe}.")
    print(f"Number of stripes: {num_stripes}.")

    # Open the file and read it.
    with open(filename, "rb") as f:
        file_data = f.read()

    # Process each stripe.
    for stripe in range(num_stripes):
        print(f"\nProcessing stripe {stripe}")

        # Read data for the stripe.
        start_byte = stripe * bytes_per_stripe
        end_byte = min(start_byte + bytes_per_stripe, filesize) # min lets us end based on filesize
        stripe_data = file_data[start_byte:end_byte]

        # # Debug prints.
        # print(f"Start byte: {start_byte}.")
        # print(f"End byte: {end_byte}.")
        # print(f"Stripe data: {stripe_data}.")

        # Split into n-1 data blocks.
        data_blocks = []
        for i in range(data_blocks_per_stripe):
            # Calculate where the blocks start and end.
            block_start = i * striping_unit
            block_end = min(block_start + striping_unit, len(stripe_data))

            if block_start < len(stripe_data):
                block = stripe_data[block_start:block_end]

                # For blocks that don't take the full striping unit, pad it with null bytes.
                if len(block) < striping_unit:
                    block += b'\x00' * (striping_unit - len(block))
            else:
                # All of the data is present in blocks, the remaining are null bytes.
                block = b'\x00' * striping_unit

            data_blocks.append(block)   

            # # Debug prints.
            # print(f"\nBlock Start: {block_start}.")
            # print(f"Stripe data length: {len(stripe_data)}.")
            # print(f"Block: {block}.")
                 

        # Compute Parity.
        parity_block = bytearray(striping_unit)
        for block in data_blocks:
            for byte in range(striping_unit):
                parity_block[byte] ^= block[byte]

        # Immutable now
        parity_block = bytes(parity_block) 

        # Determine the parity position.
        parity_position = num_drives - ((stripe % num_drives) + 1)

        # Create the actual stripe.
        stripe_blocks = []
        data_idx = 0
        for i in range(num_drives):
            if i == parity_position:
                stripe_blocks.append(('parity', parity_block))
            else:
                stripe_blocks.append(('data', data_blocks[data_idx]))
                data_idx += 1
        
        # # Debug prints.
        # print(f"{stripe_blocks[0]}")
        # print(f"{stripe_blocks[1]}")
        # print(f"{stripe_blocks[2]}")

        # Use threads to send the blocks in parallel.
        # This sends the stripe
        threads = []
        for i in range(num_drives):
            block_type, block_data = stripe_blocks[i]
            disk = disks[i]

            # Create the message.
            message = f"WRITE {filename} {stripe} {block_type} ".encode('utf-8') + block_data

            # Create the thread.
            thread = threading.Thread(
                target = send_data_block_to_disk,
                args = (sock_peer, message, disk['ip'], disk['c-port'], disk['name'], stripe, block_type)
            )
            threads.append(thread)
            thread.start()
        
        # Wait for thread to finish.
        for thread in threads:
            thread.join()

        print(f"Stripe {stripe} write successful.")

    print(f"\nFile {filename} successfully copied to {dss_name}.")
    
def send_data_block_to_disk(sock, message, ip, port, disk_name, stripe_num, block_type):    
    # Thread function to send data blocks to disk.

    try:
        sock.sendto(message, (ip, port))
        print(f"Send {block_type} block, stripe {stripe_num} to {disk_name}.")

    except Exception:
        print(f"Error sending data to {disk_name}.")

def main():
    # Syntax check
    if len(sys.argv) != 5:
        print("Please make sure you have the correct arguments.")
        print("client.py <client_port> <server_ip> <server_port> <peer_port>")
        exit(-1)

    if int(sys.argv[1]) < 13100 or int(sys.argv[1]) > 13199:
        print("Client Port - Please use a port number in the range of 13100-13199.")
        exit(-1)

    try:
        ipaddress.ip_address(sys.argv[2])
    except ValueError:
        print("Invalid server IP address")
        exit(-1)

    if int(sys.argv[3]) < 13100 or int(sys.argv[3]) > 13199:
        print("Server Port - Please use a port number in the range of 13100-13199.")
        exit(-1)

    if int(sys.argv[4]) < 13100 or int(sys.argv[4]) > 13199:
        print("Peer Port - Please use a port number in the range of 13100-13199.")
        exit(-1)

    # Create socket
    sock_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_peer = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Bind to the local IP address and UDP port
    sock_server.bind(('', int(sys.argv[1])))
    sock_peer.bind(('', int(sys.argv[4])))

    while True:
        copy = False 
        decommission = False

        # User input
        message = input("Please type the command you want to send to the manager, use \"Exit\" to exit the program: ")

        if message.strip() == "Exit" or message.strip() == "exit":
            break
        
        message_split = message.strip().split()
        if message_split[0] == "copy": copy = True
        elif message_split[0] == "decommission-dss": decommission = True
        
        # Handle copy errors
        if copy:
            try:
                if not os.path.isfile(message_split[1]):
                    print(f"'{message_split[1]}' is not a file.")
                    continue
                if not os.path.getsize(message_split[1]) > 0:
                    print(f"'{message_split[1]}' is empty.")
                    continue
                if not message_split[3]:
                    print("Please provide an owner.")
                    continue
            except IndexError:
                print("Please utilize the correct syntax for copy.")
                print("copy <filename> <filesize> <owner>")
                continue

        # Send data to server 
        sock_server.sendto(message.encode("utf-8"), (sys.argv[2], int(sys.argv[3])))

        # Wait for a server response
        data, addr = sock_server.recvfrom(1024)
        data_decoded = data.decode('utf-8')

        print(f"Server Response: {data_decoded}")

        # data_decoded = "DSS1 3 128 DISK_1 0.0.0.0 13150 DISK_2 0.0.0.0 13151 DISK_3 0.0.0.0 13152" #FIXME GET RID OF THIS 

        # Copy command
        if copy:
            if data_decoded == "FAILURE":
                print("Copy failed, continuing...")
            else:
                # Spawn threads to copy the file into the DSS's
                print(f"Copy success, continuing to copy file '{message_split[1]}'")
                data_split = data_decoded.strip().split()
                
                # Data from server: DSS_NAME NUM_DRIVES STRIPE_UNIT DISK_1 DSS_IP DSS_C-PORT DISK_2...
                dss_name = data_split[0]
                num_drives = int(data_split[1])
                striping_unit = int(data_split[2])

                # Parse disk info
                disks = []
                for i in range(num_drives):
                    idx = 3 + (i * 3)
                    disk_info = {
                        'name': data_split[idx],
                        'ip': data_split[idx + 1],
                        'c-port': int(data_split[idx + 2])
                    }
                    disks.append(disk_info)

                filename = message_split[1]
                filesize = os.path.getsize(filename) # Not using message_split[2]

                copy_file_to_dss(filename, filesize, disks, dss_name, num_drives, striping_unit, sock_peer)

                # Send data to server
                sock_server.sendto(b"copy-complete", (sys.argv[2], int(sys.argv[3])))

                # Wait for a server response
                data, addr = sock_server.recvfrom(1024)
                print(f"Server Response: {data.decode('utf-8')}")
                
        # Decommission-dss command
        if decommission:
            if data_decoded == "FAILURE":
                print("Decommission-dss failed, continuing...")
            else:
                # Spawn threads to copy the file into the DSS's
                print(f"Decommissioning DSS success'{message_split[1]}'")
                data_split = data_decoded.strip().split()
                
                # Data from server: DSS_NAME NUM_DRIVES STRIPE_UNIT DISK_1 DSS_IP DSS_C-PORT DISK_2...
                dss_name = data_split[0]
                num_drives = int(data_split[1])
                striping_unit = int(data_split[2])

                # Parse disk info
                disks = []
                for i in range(num_drives):
                    idx = 3 + (i * 3)
                    disk_info = {
                        'name': data_split[idx],
                        'ip': data_split[idx + 1],
                        'c-port': int(data_split[idx + 2])
                    }
                    disks.append(disk_info)
                    
                # Send DELETE msg for each disk
                for disks in disks:
                    deleteMSG = f"DELETE {dss_name}".encode('utf-8')
                    sock_peer.sendto(deleteMSG, (disks['ip'], disks['c-port']))
                    print(f"Delete command sent to {disks['name']}")

                 # Send complete to server
                sock_server.sendto(b"decommission-complete", (sys.argv[2], int(sys.argv[3])))

                # Wait for a server response
                data, addr = sock_server.recvfrom(1024)
                print(f"Server Response: {data.decode('utf-8')}")
                
    sock_server.close()
    sock_peer.close()
    
main()


