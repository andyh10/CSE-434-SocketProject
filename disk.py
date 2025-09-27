import socket
import ipaddress
import sys

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
    sock_server.bind(('127.0.0.1', int(sys.argv[1])))
    sock_peer = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Send data to server 
    message = input("Please type the command you want to send to the manager: ")
    sock_server.sendto(message.encode("utf-8"), (sys.argv[2], int(sys.argv[3])))

    data, addr = sock_server.recvfrom(1024)
    data_decoded = data.decode('utf-8')

    print(f"Server Response: {data_decoded}")

    sock_server.close()
    
main()
