import argparse
import socket
import sys
from utils import PacketHeader, compute_checksum


def receiver(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((receiver_ip, receiver_port))
    
    expected_seq_num = 1
    buffer = {}
    connected = False

    while True:
        try:
            pkt, address = s.recvfrom(2048)
        except Exception:
            continue

        if len(pkt) < 16:
            continue

        pkt_header = PacketHeader(pkt[:16])
        msg = pkt[16 : 16 + pkt_header.length]

        pkt_checksum = pkt_header.checksum
        pkt_header.checksum = 0
        computed_checksum = compute_checksum(pkt_header / msg)
        if pkt_checksum != computed_checksum:
            continue

        pkt_type = pkt_header.type
        seq_num = pkt_header.seq_num

        if pkt_type == 0:
            expected_seq_num = 1
            buffer.clear()
            connected = True
            ack_header = PacketHeader(type=3, seq_num=1, length=0)
            ack_header.checksum = compute_checksum(ack_header)
            s.sendto(bytes(ack_header), address)

        elif pkt_type == 2 and connected:
            if seq_num >= expected_seq_num + window_size:
                continue

            if seq_num >= expected_seq_num:
                if seq_num not in buffer:
                    buffer[seq_num] = msg

            while expected_seq_num in buffer:
                sys.stdout.buffer.write(buffer[expected_seq_num])
                sys.stdout.buffer.flush()
                del buffer[expected_seq_num]
                expected_seq_num += 1


            ack_header = PacketHeader(type=3, seq_num=seq_num, length=0)
            ack_header.checksum = compute_checksum(ack_header)
            s.sendto(bytes(ack_header), address)

        elif pkt_type == 1 and connected:
            ack_header = PacketHeader(type=3, seq_num=seq_num, length=0)
            ack_header.checksum = compute_checksum(ack_header)
            s.sendto(bytes(ack_header), address)
            break

    s.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip", help="The IP address of the host that receiver is running on")
    parser.add_argument("receiver_port", type=int, help="The port number on which receiver is listening")
    parser.add_argument("window_size", type=int, help="Maximum number of outstanding packets")
    args = parser.parse_args()

    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
