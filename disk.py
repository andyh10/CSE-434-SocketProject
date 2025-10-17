import socket
import ipaddress
import sys

def handle_copy_message(sock_peer, storage):
    # Handle incoming peer copy messages.
    # Use in a thread.

    while True:
        try:
            data, addr = sock_peer.recvfrom(65536) # Large buffer for data.

            # Message format: WRITE <filename> <stripe_num> <block_type> <block_data>
            message_split = data.split()
            command = message_split[0].decode('utf-8')

            # Handle WRITE Command
            if command == "WRITE":
                filename = message_split[1].decode('utf-8')
                stripe_num = message_split[2].decode('utf-8')
                block_type = message_split[3].decode('utf-8')
                block_data = message_split[4].decode('utf-8')

                if filename not in storage:
                    storage[filename] = {}
                storage[filename][stripe_num] = {
                    'type': block_type,
                    'data': block_data
                }

                print(f"Stored {block_type} block for stripe {stripe_num} of file {filename}")

        except Exception:
            print("Error handling peer copy message.")  

def main():
    # Syntax check
    if len(sys.argv) != 5:
        print("Please make sure you have the correct arguments.")
        print("disk.py <disk_port> <server_ip> <server_port> <peer_port>")
        exit(-1)

    if int(sys.argv[1]) < 13100 or int(sys.argv[1]) > 13199:
        print("Disk Port - Please use a port number in the range of 13100-13199.")
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
    sock_peer = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Send data to server 
    message = input("Please type the command you want to send to the manager: ")
    sock_server.sendto(message.encode("utf-8"), (sys.argv[2], int(sys.argv[3])))

    data, addr = sock_server.recvfrom(1024)
    data_decoded = data.decode('utf-8')

    print(f"Server Response: {data_decoded}")

    sock_server.close()
    
main()

