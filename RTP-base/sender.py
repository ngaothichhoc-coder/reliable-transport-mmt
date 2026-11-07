import argparse
import socket
import sys
import time
from utils import PacketHeader, compute_checksum


def sender(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver_addr = (receiver_ip, receiver_port)

    # Đọc dữ liệu từ stdin (hỗ trợ cả dữ liệu nhị phân)
    message = sys.stdin.buffer.read()
    
    # Chia nhỏ dữ liệu thành các chunk (mỗi chunk tối đa 1000 bytes để an toàn dưới giới hạn 1472 bytes)
    chunk_size = 1000
    chunks = [message[i:i + chunk_size] for i in range(0, len(message), chunk_size)]
    total_packets = len(chunks)

    # 1. Gửi gói tin START (type = 0, seq_num = 0)
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
        # Nếu message rỗng, gửi ngay gói END
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

    # Chuẩn bị danh sách gói tin DATA (seq_num bắt đầu từ 1)
    packets = {}
    for idx, chunk in enumerate(chunks):
        seq_num = idx + 1
        hdr = PacketHeader(type=2, seq_num=seq_num, length=len(chunk))
        hdr.checksum = compute_checksum(hdr / chunk)
        packets[seq_num] = bytes(hdr / chunk)

    base = 1
    next_seq_num = 1
    s.settimeout(0.02) # Timeout ngắn để kiểm tra ACK không chặn luồng
    timer_start = time.time()

    # 2. Truyền dữ liệu với cơ chế Sliding Window và Timer 500ms
    while base <= total_packets:
        # Gửi các gói tin nằm trong giới hạn window
        while next_seq_num < base + window_size and next_seq_num <= total_packets:
            s.sendto(packets[next_seq_num], receiver_addr)
            next_seq_num += 1

        # Nhận ACK phản hồi từ receiver
        try:
            pkt, _ = s.recvfrom(2048)
            if len(pkt) >= 16:
                ack_header = PacketHeader(pkt[:16])
                if ack_header.type == 3:
                    ack_seq = ack_header.seq_num
                    if ack_seq > base:
                        base = ack_seq
                        timer_start = time.time() # Reset timer khi window dịch chuyển
        except socket.timeout:
            pass

        # Kiểm tra timeout 500ms (0.5 giây)
        if time.time() - timer_start > 0.5:
            # Retransmit lại toàn bộ các gói tin trong window hiện tại
            for seq in range(base, next_seq_num):
                s.sendto(packets[seq], receiver_addr)
            timer_start = time.time()

    # 3. Gửi gói tin END và đợi ACK hoặc tối đa 500ms
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
