import asyncio
import pickle
from congregation.net.messages import *


class Handler:
    def __init__(self, peer, server: [asyncio.Protocol, None] = None):
        self.peer = peer
        self.server = server
        self.msg_handlers = self._define_msg_map()

    def handle_msg(self, data):
        """
        Determine message type and handle accordingly
        """

        if isinstance(data, Msg):
            m = data
        else:
            m = pickle.loads(data)

        if m.pid not in self.peer.peer_connections:
            raise Exception(f"Msg of type {m.msg_type} received from unrecognized peer: {m.pid}")

        self.msg_handlers[m.msg_type](m)

    def _define_msg_map(self):
        return {
            "IAM": self.handle_iam_msg,
            "READY": self.handle_ready_msg,
            "CONFIG": self.handle_config_msg,
            "ACK": self.handle_ack_msg
        }

    def handle_iam_msg(self, m: IAMMsg):
        """
        we need to be able to resolve which party a given connection
        is for, which is why a done callback is added to the connection
        future which sends an IAMMsg with the pid of the connecting party.
        this function sets that connection value in peer.peer_connections
        accordingly when an IAMMsg is received.
        """

        print(f"IAMMsg received from {m.pid}")
        conn = self.peer.peer_connections[m.pid]
        if isinstance(conn, asyncio.Future):
            conn.set_result((self.server.transport, self))

    def handle_ready_msg(self, m: ReadyMsg):
        pass

    def handle_config_msg(self, m: ConfigMsg):

        if self.peer.dispatcher is not None:
            if self.peer.dispatcher.dispatch_type == m.job_type:

                print(f"ConfigMsg received from party {m.pid} for {m.job_type} job.")
                cfg = self.peer.dispatcher.parties_config[m.pid]["CFG"]
                if isinstance(cfg, asyncio.Future):
                    cfg.set_result(m.config)

                print(f"Sending AckMsg to party {m.pid} for receipt of ConfigMsg for {m.job_type} job.")
                self.peer.send_ack(
                    m.pid,
                    "CONFIG",
                    m.job_type
                )

                return

        self.peer.msg_buffer.append(m)

    def handle_ack_msg(self, m: AckMsg):
        """
        TODO: won't want to have an if/elif/.../else chain here
          if we have ack messages for things other than config
          in the future, need to design something more elegant
            - could have the futures dicts on peer.dispatcher
            keyed on types (e.g. CONFIG, <other_type>, etc.)
            and then do a lookup based on the type from the ack
        """

        if self.peer.dispatcher is not None:
            if self.peer.dispatcher.dispatch_type == m.job_type:

                print(
                    f"AckMsg of type {m.ack_type} received from "
                    f"party {m.pid} for {m.job_type} job."
                )
                if m.ack_type == "CONFIG":
                    a = self.peer.dispatcher.parties_config[m.pid]["ACK"]
                    if isinstance(a, asyncio.Future):
                        a.set_result(True)
                    return
