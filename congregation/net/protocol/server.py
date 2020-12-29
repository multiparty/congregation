import asyncio
from congregation.net.protocol.protocol import Protocol
from congregation.net.messages import *


class ServerProtocol(Protocol):
    def __init__(self, peer):
        super().__init__(peer)

    def connection_made(self, transport: asyncio.transports.BaseTransport):

        peername = transport.get_extra_info("peername")
        print("Server connection made from: {}".format(peername))
        self.transport = transport

    def _handle_iam_msg(self, msg: IAMMsg):

        if msg.pid not in self.peer.peer_connections:
            raise Exception(f"Unrecognized peer attempting to register: {msg.pid}")

        print(f"IAMMsg received from {msg.pid}")
        conn = self.peer.peer_connections[msg.pid]
        if isinstance(conn, asyncio.Future):
            conn.set_result((self.transport, self))

    def _handle_ready_msg(self, msg: ReadyMsg):
        pass

    def _handle_config_msg(self, msg: ConfigMsg):

        if self.peer.dispatcher is not None:
            if self.peer.dispatcher.dispatch_type == msg.job_type:

                print(f"ConfigMsg received from party {msg.pid} for {msg.job_type} job.")
                cfg = self.peer.dispatcher.parties_config[msg.pid]["CFG"]

                print(f"Sending AckMsg to party {msg.pid} for receipt of ConfigMsg for {msg.job_type} job.")
                self.peer.send_ack(
                    self.transport,
                    "CONFIG",
                    msg.job_type
                )

                print(f"Sending ConfigMsg to party {msg.pid} for {msg.job_type} job.")
                self.peer.send_cfg(
                    self.transport,
                    self.peer.dispatcher.config_to_exchange,
                    self.peer.dispatcher.dispatch_type
                )

                if isinstance(cfg, asyncio.Future):
                    cfg.set_result(msg.config)
                return

        self.peer.msg_buffer.append(msg)
