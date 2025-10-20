import socket
import ipaddress
import sys
import os
import threading
import math
import random
import subprocess

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

        # Debug prints.
        print(f"Start byte: {start_byte}.")
        print(f"End byte: {end_byte}.")
        print(f"Stripe data: {stripe_data}.")

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

            # Debug prints.
            print(f"\nBlock Start: {block_start}.")
            print(f"Stripe data length: {len(stripe_data)}.")
            print(f"Block: {block}.")        

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
        
        # Debug prints.
        print(f"{stripe_blocks[0]}")
        print(f"{stripe_blocks[1]}")
        print(f"{stripe_blocks[2]}")

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

def read_file_from_dss(filename, filesize, disks, dss_name, num_drives, striping_unit, sock_peer, error_p=0):
    # Set the error_p to however much desired.
    # Reads a file from DSS using block-interleaved distributed parity.

    print(f"Reading {filename} size {filesize} B from {dss_name}")
    print(f"with error probability of {error_p}%.")

    # Calculate amount of stripes required for file.
    # This calculation is required to read the blocks.
    data_blocks_per_stripe = num_drives - 1
    bytes_per_stripe = data_blocks_per_stripe * striping_unit
    num_stripes = math.ceil(filesize/bytes_per_stripe)
 
    print(f"\n-- STRIPING CALCULATIONS for '{filename}' --")
    print(f"Data blocks per stripe: {data_blocks_per_stripe}.")
    print(f"Bytes per stripe: {bytes_per_stripe}.")
    print(f"Number of stripes: {num_stripes}.")

    # Create output file for writing to.
    output_file = f"read-{filename}"
    output_data = bytearray()
    
    for stripe in range(num_stripes):
        print(f"\nReading stripe {stripe}...")

        stripe_verified = False
        retry_count = 0
        max_retry = 5

        # Iterate only until either stripe read is correct or retries exceeeded.
        while not stripe_verified and retry_count < max_retry:
            if retry_count > 0:
                print(f"Retry number {retry_count} for stripe {stripe}")

            # Use threads to read blocks in parallel.
            stripe_block = [None] * num_drives # Initiate stripe block to nothing, use for checking.
            threads = []

            for drive in range(num_drives):
                disk = disks[drive]

                # Create the thread.
                thread = threading.Thread(
                    target=read_block_from_disk,
                    args=(sock_peer, filename, stripe, drive, disk, stripe_block)
                )
                threads.append(thread)
                thread.start()

            # Wait for thread to finish.
            for thread in threads:
                thread.join()

            if None in stripe_block:
                print(f" Error: Failed to read block succesfully for stripe {stripe}")
                retry_count += 1
                continue

            # Introduce bit error.
            for block_idx in range(num_drives):
                stripe_block[block_idx] = bit_error(stripe_block[block_idx], error_p)

            # Verify Parity.
            parity_pos = num_drives - ((stripe % num_drives) + 1)
            parity_verified = verify_parity(stripe_block, parity_pos, striping_unit)

            if parity_verified:
                print(f"Stripe {stripe} parity verified.")
                stripe_verified = True

                # Write the data to output_data.
                data_idx = 0
                for drive in range(num_drives):
                    if drive != parity_pos:
                        block = stripe_block[drive]

                        # Calculate the remaining bytes to write.
                        bytes_remaining = filesize - len(output_data)
                        if bytes_remaining > 0:
                            bytes_to_write = min(len(block), bytes_remaining)
                            print(f"DEBUG: Writing block from drive {drive}, parity_pos={parity_pos}, block preview: {block[:20]}")
                            output_data.extend(block[:bytes_to_write]) # Use .extend to write over all of the bytes from block.
                        
                        data_idx += 1

            else:
                print(f"Stripe {stripe} failed parity verification. Retrying...")
                retry_count += 1

        if not stripe_verified:
            print(f"Failed to verify stripe {stripe} after {max_retry - 1} retries.")
            return
        
    # Write to the output file.
    with open(output_file, 'wb') as f:
        f.write(output_data)

    print(f"\nFile {filename} read successfully. Output written to {output_file}")

    # Verify by using diff.
    result = subprocess.run(
        ['diff', filename, output_file],
        capture_output = True,
        text = True
    )

    if result.returncode == 0:
        print(f"Output matches with original, verified by 'diff'.")
    else:
        print(f"Output does not match with original file.")
        print(result.stdout)

def read_block_from_disk(sock, filename, stripe_num, block_idx, disk, stripe_block):
    # Thread function to read data blocks from disk.

    try:
        message = f"READ {filename} {stripe_num} {block_idx}".encode('utf-8')
        sock.sendto(message, (disk['ip'], disk['c-port']))

        data, addr = sock.recvfrom(65536)

        stripe_block[block_idx] = data
        print(f"Received block {block_idx} from {disk['name']}, stripe {stripe_num}.")

    except Exception:
        print(f"Error reading block {block_idx} from {disk['name']}.")

def verify_parity(stripe_block, parity_pos, striping_unit):
    # Helper function to verify parity for a block.

    computed_parity = bytearray(striping_unit)

    # Compute the parity.
    for i in range(len(stripe_block)):
        if i != parity_pos:
            block = stripe_block[i]
            for byte in range(striping_unit):
                computed_parity[byte] ^= block[byte]    

    computed_parity = bytes(computed_parity)
    actual_parity = stripe_block[parity_pos]

    if computed_parity == actual_parity:
        return True
    else:
        return False

def bit_error(block, error_p):
    # Helper function to introduce bit error for a block.

    rand = random.randint(0, 100)

    # If random integer is less than the error percentage, introduce the bit error.
    if rand < error_p:
        
        block_array = bytearray(block)
        random_byte = random.randint(0, len(block_array) - 1)   # Select a random byte.
        random_bit = random.randint(0, 7)                       # Select a random bit.
        block_array[random_byte] ^= (1 << random_bit)           # Flip that bit.

        return bytes(block_array)

    return block

def simulate_disk_failure(disks, dss_name, num_drives, striping_unit, sock_peer, storage):
    # Simulates a failure on a DSS.

    print(f"Simulating failure on {dss_name}...")

    # Select a random disk.
    failed_disk_idx = random.randint(0, num_drives - 1)
    failed_disk = disks[failed_disk_idx]

    # Send message to disk to fail.
    print(f"Selected disk {failed_disk_idx} - {failed_disk['name']} to fail.")
    sock_peer.sendto(b"FAIL", (failed_disk['ip'], failed_disk['c-port']))

    # Check for correct message back.
    data, addr = sock_peer.recvfrom(1024)
    if data == b"fail-complete":
        print(f"Received fail-complete from {failed_disk['name']}, continuing...")
    else:
        print(f"Did not receive fail-complete from {failed_disk['name']}, exiting...")
        return
    
    # Reconstruct the disk.
    print(f"\nReconstructing disk {failed_disk_idx}...")

    # Get a list of disks that work.
    working_disks = []
    for i in range(num_drives):
        if i != failed_disk_idx:
            working_disks.append(i) 

    # For each file that is a part of the DSS
    for filename in storage:
        print(f" Reconstructing file '{filename}")

        working_disk_idx = working_disks[0]
        working_disk = disks[working_disk_idx]

        stripes_found = []
        stripe_num = 0

        # Find stripes by reading from working disk.
        while stripe_num < 1000:
            message = f"READ {filename} {stripe_num} {working_disk_idx}".encode('utf-8')
            sock_peer.sendto(message, (working_disk['ip'], working_disk['c-port']))

            block_data, _ = sock_peer.recvfrom(65536)
            if block_data == b"BLOCK NOT FOUND":
                break
            else:
                stripes_found.append(stripe_num)
                stripe_num += 1

        # For each stripe of the file
        for stripe in stripes_found:
            # Find the parity.
            parity_pos = num_drives - ((int(stripe) % num_drives) + 1)

            # Read blocks
            blocks = []
            for disk_idx in working_disks:
                disk = disks[disk_idx]

                # Request block
                message = f"READ {filename} {stripe} {disk_idx}".encode('utf-8')
                sock_peer.sendto(message, (disk['ip'], disk['c-port']))

                block_data, addr = sock_peer.recvfrom(65536)
                blocks.append(block_data)

            # Compute the new block.
            computed_block = bytearray(striping_unit)
            for block in blocks:
                # Pad if necessary
                if len(block) < striping_unit:
                    block = block + b'\x00' * (striping_unit - len(block))

                for i in range(striping_unit):
                    computed_block[i] ^= block[i]
            computed_block = bytes(computed_block)

            # Determine the type of block.
            if failed_disk_idx == parity_pos:
                block_type = "parity"
            else:
                block_type = "data"

            # Send the new block to failed disk.
            message = f"WRITE {filename} {stripe} {block_type} ".encode('utf-8') + computed_block
            sock_peer.sendto(message, (failed_disk['ip'], failed_disk['c-port']))

            print(f"Restored {block_type} block to stripe {stripe}.")

    print(f"Disk {failed_disk_idx}, name '{failed_disk['name']}' reconstruction complete.")

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

        # Read command
        if message_split[0] == "read":
            if not data_decoded.startswith("FAILURE"):
                read_split = data_decoded.split()

                # Data from server: FILESIZE, DSSNAME, NUM_DRIVES, STRIPING_UNIT DISK_1 DISK_IP DISK_C-PORT DISK_2...
                filename = message_split[2]
                filesize = int(read_split[0])
                dssname = read_split[1]
                num_drives = int(read_split[2])
                striping_unit = int(read_split[3])

                # Parse disk info
                disks = []
                for i in range(num_drives):
                    idx = 4 + (i * 3)
                    disk_info = {
                        'name': read_split[idx],
                        'ip': read_split[idx + 1],
                        'c-port': int(read_split[idx + 2])
                    }
                    disks.append(disk_info)

                read_file_from_dss(filename, filesize, disks, dssname, num_drives, striping_unit, sock_peer, 0)

                # Send data to server
                sock_server.sendto(b"read-complete", (sys.argv[2], int(sys.argv[3])))

                # Wait for a server response
                data, addr = sock_server.recvfrom(1024)
                print(f"Server Response: {data.decode('utf-8')}")

            else:
                print("Read failed, continuing...")

        # Disk-Failure command
        if message_split[0] == "disk-failure":
            disk_split = data_decoded.split()
            if len(message_split) != 2:
                print("Make sure the syntax for disk-failure is correct.")
                continue
            dss_name = message_split[1]

            if data_decoded.startswith("FAILURE"):
                print(f"disk-failure failed for {dss_name}")
            else:

                # Data from server: DSSNAME NUM_DRIVES STRIPING_UNIT DISK1 DISK_IP DISK_C-PORT DISK2...
                dssname = disk_split[0]
                num_drives = int(disk_split[1])
                striping_unit = int(disk_split[2])

                # Parse disk info
                disks = []
                storage = []
                file_idx = 0
                for i in range(num_drives):
                    idx = 3 + (i * 3)

                    file_idx = idx

                    disk_info = {
                        'name': disk_split[idx],
                        'ip': disk_split[idx + 1],
                        'c-port': int(disk_split[idx + 2])
                    }
                    disks.append(disk_info)

                # Add all files to a list.
                if file_idx != -1:
                    for i in range(file_idx+4, len(disk_split)):
                        print(f"{disk_split[i]}")
                        storage.append(disk_split[i])

                print(f"{storage}")

                simulate_disk_failure(disks, dss_name, num_drives, striping_unit, sock_peer, storage)

                # Send recovery-complete message and wait for response.
                sock_server.sendto(b"recovery-complete", addr)
                data, addr = sock_server.recvfrom(1024)
        
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
                for disk in disks:
                    deleteMSG = f"DELETE {dss_name}".encode('utf-8')
                    sock_peer.sendto(deleteMSG, (disk['ip'], disk['c-port']))
                    print(f"Delete command sent to {disk['name']}")

                 # Send complete to server
                sock_server.sendto(b"decommission-complete", (sys.argv[2], int(sys.argv[3])))

                # Wait for a server response
                data, addr = sock_server.recvfrom(1024)
                print(f"Server Response: {data.decode('utf-8')}")
                
    sock_server.close()
    sock_peer.close()
    
main()









