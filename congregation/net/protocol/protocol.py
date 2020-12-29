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
        """ Overridden in subclasses """
        pass

    def connection_lost(self, exc):
        print('The server closed the connection')

    def data_received(self, data: bytes):
        """
        Add incoming messages to buffer and process them
        """
        self.buffer += data
        self.handle_lines()

    def handle_lines(self):
        """
        Process messages from buffer
        """
        while b"\n\n\n" in self.buffer:
            data, self.buffer = self.buffer.split(b"\n\n\n", 1)
            self.handle_msg(data)

    def handle_msg(self, data):
        """
        Determine message type and handle accordingly
        """
        msg = pickle.loads(data)
        if msg.pid not in self.peer.peer_connections:
            raise Exception(f"ConfigMsg received from unrecognized peer: {msg.pid}")

        msg_handler = self.msg_handlers[msg.msg_type]
        msg_handler(msg)

    def define_msg_map(self):
        return {
            "IAM": self._handle_iam_msg,
            "READY": self._handle_ready_msg,
            "CONFIG": self._handle_config_msg,
            "ACK": self._handle_ack_msg
        }

    def _handle_iam_msg(self, msg: IAMMsg):
        pass

    def _handle_ready_msg(self, msg: ReadyMsg):
        pass

    def _handle_config_msg(self, msg: ConfigMsg):
        pass

    def _handle_ack_msg(self, msg: AckMsg):

        if self.peer.dispatcher is not None:
            if self.peer.dispatcher.dispatch_type == msg.job_type:
                print(f"AckMsg of type {msg.ack_type} received from party {msg.pid} for {msg.job_type} job.")
                if msg.ack_type == "CONFIG":
                    ack = self.peer.dispatcher.parties_config[msg.pid]["ACK"]
                    if isinstance(ack, asyncio.Future):
                        ack.set_result(True)
                    return
        self.peer.msg_buffer.append(msg)
