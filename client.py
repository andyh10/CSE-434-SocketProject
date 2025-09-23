import socket
import ipaddress

def main():
    # Create socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Bind to the local IP address and UDP port
    while True:
        ip_addr = input("Please input your device's IP: ").strip()
    
        # Make sure that the IP address is correct.
        try:
            ipaddress.ip_address(ip_addr)
            break
        except ValueError:
            print("Invalid IP address input.")

    while True:
        port_num = int(input("Please input the port number you want to bind: "))

        # Make sure it is in the right port range.
        if port_num >= 13100 and port_num <= 13199:
            break
        else:
            print("Please use a port number in the range of 13100-13199.")  

    sock.bind((ip_addr, port_num))

    # Send data to server
    while True:
        ip_addr_manager = input("Please input the manager process's IP: ").strip()
    
        # Make sure that the IP address is correct.
        try:
            ipaddress.ip_address(ip_addr_manager)
            break
        except ValueError:
            print("Invalid IP address input.")
    
    while True:
        port_num_manager = int(input("Please input manager process's port: "))

        # Make sure it is in the right port range.
        if port_num_manager >= 13100 and port_num_manager <= 13199:
            break
        else:
            print("Please use a port number in the range of 13100-13199.") 

    message = input("Please type the command you want to send to the manager")
    sock.sendto(message.encode('utf-8'), (ip_addr_manager, port_num_manager))

    sock.close()
    
main()
