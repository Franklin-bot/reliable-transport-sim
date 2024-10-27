# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct
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

        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(self.listener)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!

        for i in range(0, len(data_bytes), self.packet_size):
            curr_bytes = data_bytes[i:min(i+self.packet_size, len(data_bytes))]
            segment = struct.pack(f"!I{len(curr_bytes)}s", self.seq_num, curr_bytes)
            self.seq_num += min(len(curr_bytes), self.packet_size)
            self.socket.sendto(segment, (self.dst_ip, self.dst_port))
        


    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        
        # segment, addr = self.socket.recvfrom()
        # tup = struct.unpack(f"!I{len(segment)-4}s", segment)
        # seq_num = tup[0]
        # data = tup[1]

        res = bytearray()
        # self.receive_buffer[seq_num] = data

        while (self.expected in self.receive_buffer):
            buffer_segment = self.receive_buffer.pop(self.expected)
            res += buffer_segment
            self.expected += len(buffer_segment)

        return bytes(res)






    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        self.closed = True
        self.socket.stoprecv()
        pass



    def listener(self) -> None:

        while not self.closed:
            print(self.receive_buffer)
            try:
                segment, addr = self.socket.recvfrom()
                tup = struct.unpack(f"!I{len(segment)-4}s", segment)
                seq_num = tup[0]
                data = tup[1]
                self.receive_buffer[seq_num] = data
            except Exception as e:
                print("listener died!")
                print(e)

