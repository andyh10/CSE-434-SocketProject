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

    # Data 
    while True:
        data, addr = sock.recvfrom(1024)

        # FIXME implement proper command handling.
        print(f"Received: {data.decode('utf-8')}")
        print(f"From: {addr}")

        sock.sendto(b"I received your message.", addr)
main()
