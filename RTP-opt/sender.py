import argparse
import socket
import sys
import time
from utils import PacketHeader, compute_checksum


def sender(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver_addr = (receiver_ip, receiver_port)

    message = sys.stdin.buffer.read()
    
    chunk_size = 1000
    chunks = [message[i:i + chunk_size] for i in range(0, len(message), chunk_size)]
    total_packets = len(chunks)

    start_header = PacketHeader(type=0, seq_num=0, length=0)
    start_header.checksum = compute_checksum(start_header)
    
    start_acked = False
    s.settimeout(0.1)
    while not start_acked:
        s.sendto(bytes(start_header), receiver_addr)
        try:
            pkt, _ = s.recvfrom(2048)
            if len(pkt) >= 16:
                ack_header = PacketHeader(pkt[:16])
                if ack_header.type == 3 and ack_header.seq_num == 1:
                    start_acked = True
        except socket.timeout:
            continue

    if total_packets == 0:
        end_header = PacketHeader(type=1, seq_num=1, length=0)
        end_header.checksum = compute_checksum(end_header)
        s.settimeout(0.5)
        for _ in range(5):
            s.sendto(bytes(end_header), receiver_addr)
            try:
                pkt, _ = s.recvfrom(2048)
                if len(pkt) >= 16:
                    ack_header = PacketHeader(pkt[:16])
                    if ack_header.type == 3:
                        break
            except socket.timeout:
                continue
        s.close()
        return

    packets = {}
    acked = {}
    for idx, chunk in enumerate(chunks):
        seq_num = idx + 1
        hdr = PacketHeader(type=2, seq_num=seq_num, length=len(chunk))
        hdr.checksum = compute_checksum(hdr / chunk)
        packets[seq_num] = bytes(hdr / chunk)
        acked[seq_num] = False

    base = 1
    next_seq_num = 1
    s.settimeout(0.02)
    timer_start = time.time()

    while base <= total_packets:
        while next_seq_num < base + window_size and next_seq_num <= total_packets:
            s.sendto(packets[next_seq_num], receiver_addr)
            next_seq_num += 1

        try:
            pkt, _ = s.recvfrom(2048)
            if len(pkt) >= 16:
                ack_header = PacketHeader(pkt[:16])
                if ack_header.type == 3:
                    ack_seq = ack_header.seq_num
                    if ack_seq in packets and not acked[ack_seq]:
                        acked[ack_seq] = True
                        old_base = base
                        while base < next_seq_num and acked.get(base, False):
                            base += 1
                        
                        if base > old_base:
                            timer_start = time.time()
        except socket.timeout:
            pass
            
        if time.time() - timer_start > 0.5:
            for seq in range(base, next_seq_num):
                if not acked.get(seq, False):
                    s.sendto(packets[seq], receiver_addr)
            timer_start = time.time()

    end_seq_num = total_packets + 1
    end_header = PacketHeader(type=1, seq_num=end_seq_num, length=0)
    end_header.checksum = compute_checksum(end_header)
    
    end_time = time.time()
    s.settimeout(0.05)
    while time.time() - end_time < 0.5:
        s.sendto(bytes(end_header), receiver_addr)
        try:
            pkt, _ = s.recvfrom(2048)
            if len(pkt) >= 16:
                ack_header = PacketHeader(pkt[:16])
                if ack_header.type == 3:
                    break
        except socket.timeout:
            continue

    s.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip", help="The IP address of the host that receiver is running on")
    parser.add_argument("receiver_port", type=int, help="The port number on which receiver is listening")
    parser.add_argument("window_size", type=int, help="Maximum number of outstanding packets")
    args = parser.parse_args()

    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
