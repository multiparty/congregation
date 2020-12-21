import asyncio
import pickle
from congregation.net.messages import *


class Protocol(asyncio.Protocol):
    def __init__(self, peer):
        self.peer = peer
        self.buffer = b""
        self.transport = None
        self.msg_handlers = self.define_msg_map()

    def connection_made(self, transport: asyncio.transports.BaseTransport):

        # get_extra_info("peername") returns (<IP_ADDR>, <INT>)
        # tuple, and idk what the <INT> is
        peername = transport.get_extra_info("peername")[0]
        print(f"Connection made from {peername}")
        self.transport = transport

    def data_received(self, data: bytes):
        """
        Add incoming messages to buffer and process them
        """
        self.buffer += data
        self.handle_lines()

    def define_msg_map(self):

        return {
            "IAM": self._handle_iam_msg
        }

    def handle_lines(self):
        """
        Process messages from buffer
        """
        while b"\n\n\n" in self.buffer:
            all_msgs = self.buffer.split(b"\n\n\n")
            data = all_msgs[0]
            self.buffer = all_msgs[1:]
            self.handle_msg(data)

    def handle_msg(self, data):
        """
        Determine message type and handle accordingly
        """
        msg = pickle.loads(data)
        msg_handler = self.msg_handlers[msg.msg_type]
        msg_handler(msg)

    def _handle_iam_msg(self, msg: IAMMsg):

        if msg.pid not in self.peer.peer_connections:
            raise Exception(f"Unrecognized peer attempting to register: {msg.pid}")

        print(f"IAMMsg received from {msg.pid}")
        conn = self.peer.peer_connections[msg.pid]
        if isinstance(conn, asyncio.Future):
            conn.set_result((self.transport, self))
