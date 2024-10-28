import struct
import time
import hashlib
# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
from concurrent.futures import ThreadPoolExecutor


class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        self.seq_num = 0
        self.expected = 0
        self.packet_size = 1000
        self.receive_buffer = {}
        self.closed = False
        self.ack = False
        self.fin = False

        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(self.listener)

    def send(self, data_bytes: bytes) -> None:

        for i in range(0, len(data_bytes), self.packet_size):
            curr_bytes = data_bytes[i:min(i+self.packet_size, len(data_bytes))]
            hashed_bytes = hashlib.md5(curr_bytes).digest()
            segment = struct.pack(f"!III16s{len(curr_bytes)}s", self.seq_num, 0, 0, hashed_bytes, curr_bytes)
            self.seq_num += min(len(curr_bytes)+16, self.packet_size)

            while True:
                self.socket.sendto(segment, (self.dst_ip, self.dst_port))
                print("waiting for ack")
                if self.waitForAck(): break
            self.ack = False

    def waitForAck(self):
        for _ in range(0, 25):
            time.sleep(0.01)
            if (self.ack) : return True
        print("resending")
        return False


    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        
        res = bytearray()

        while (self.expected in self.receive_buffer):
            buffer_segment = self.receive_buffer.pop(self.expected)
            res += buffer_segment
            self.expected += len(buffer_segment)


        return bytes(res)

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.


        # send fin
        print("sending fin")
        while True:
            segment = struct.pack(f"!III16s1s", self.seq_num, 0, 1, b'\x00' * 16, b'')
            self.socket.sendto(segment, (self.dst_ip, self.dst_port))

            if self.waitForAck(): break
        self.ack = False

        while not self.fin:
            time.sleep(0.01)

        time.sleep(2)
        self.closed = True
        self.socket.stoprecv()
        pass

    def listener(self) -> None:

        while not self.closed:
            try:
                segment, addr = self.socket.recvfrom()
                if not segment:
                    break
                tup = struct.unpack(f"!III16s{len(segment)-28}s", segment)
                seq_num = tup[0]
                is_ack = tup[1]
                fin = tup[2]
                hashed_data = tup[3]
                data = tup[4]

                if is_ack:
                    if fin:
                        print("fin ack recieved")
                    else:
                        print("data ack recieved")
                    self.ack = True

                elif fin:
                    print("fin recieved")
                    self.fin = True
                    print("sending fin ack")
                    segment = struct.pack(f"!III16s1s", self.seq_num, 1, 1, b'\x00' * 16, b'')
                    self.socket.sendto(segment, (self.dst_ip, self.dst_port))
                else:
                    print("data recieved")
                    rehashed_data = hashlib.md5(data).digest()
                    if rehashed_data != hashed_data: 
                        print("hashed data does not match, bit has been flipped")
                        continue
                    self.receive_buffer[seq_num] = data
                    print("sending ack")
                    segment = struct.pack(f"!III16s1s", self.seq_num, 1, 0, b'\x00' * 16, b'')
                    self.socket.sendto(segment, (self.dst_ip, self.dst_port))

            except Exception as e:
                print("listener died!")
                print(e)

