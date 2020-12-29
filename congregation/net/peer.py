import asyncio
import pickle
from functools import partial
from congregation.config import Config
from congregation.net.messages import *
from congregation.net.protocol import ClientProtocol, ServerProtocol
from congregation.dispatch.dispatcher import Dispatcher


class Peer:
    def __init__(self, loop, cfg: Config):
        self.loop = loop
        self.pid = cfg.system_configs["CODEGEN"].pid
        self.parties = cfg.system_configs["NETWORK"].network_dict["parties"]
        self.host = self.parties[self.pid]["host"]
        self.port = self.parties[self.pid]["port"]
        self.peer_connections = {}
        self.msg_buffer = []
        self.server = self.loop.create_server(
            lambda: ServerProtocol(self),
            host=self.host,
            port=self.port
        )
        self.loop.run_until_complete(self.server)
        self.dispatcher = None

    def register_dispatcher(self, dispatcher: Dispatcher):

        self.dispatcher = dispatcher
        ret = []
        # TODO: might want to match instead on a job_id or something
        for msg in self.msg_buffer:
            if msg.job_type == dispatcher.dispatch_type:
                self.dispatcher.receive_msg(msg)
                continue
            ret.append(msg)
        self.msg_buffer = ret

    async def _create_connection(self, f, other_host, other_port):

        while True:
            try:
                conn = await self.loop.create_connection(f, other_host, other_port)
                return conn
            except OSError:
                print(f"Retrying connection to {other_host}:{other_port}")
                await asyncio.sleep(1)

    def connect_to_others(self):
        """
        Establish connections with all parties
        present in network configuration dict
        """
        to_wait_on = []
        for other_pid in self.parties.keys():
            if other_pid < self.pid:
                print(f"Will connect to {other_pid}")
                conn = asyncio.ensure_future(
                    self._create_connection(
                        lambda: ClientProtocol(self),
                        self.parties[other_pid]["host"],
                        self.parties[other_pid]["port"]
                    )
                )
                self.peer_connections[other_pid] = conn
                conn.add_done_callback(partial(self.send_iam))
                to_wait_on.append(conn)
            elif other_pid > self.pid:
                print(f"Will wait for {other_pid} to connect.")
                connection_made = asyncio.Future()
                self.peer_connections[other_pid] = connection_made
                to_wait_on.append(connection_made)
            else:
                # self
                continue

        self.loop.run_until_complete(asyncio.gather(*to_wait_on))
        for pid in self.peer_connections.keys():
            completed_future = self.peer_connections[pid]
            self.peer_connections[pid] = completed_future.result()[0]

    @staticmethod
    def _send_msg(transport, msg):

        formatted = pickle.dumps(msg) + b"\n\n\n"
        transport.write(formatted)

    def send_iam(self, conn):

        msg = IAMMsg(self.pid)
        if isinstance(conn, asyncio.Future):
            transport, protocol = conn.result()
        else:
            transport = conn
        self._send_msg(transport, msg)

    def send_cfg(self, conn, cfg, job_type):

        msg = ConfigMsg(self.pid, cfg, job_type)
        self._send_msg(conn, msg)

    def send_ack(self, conn, ack_type, job_type):

        msg = AckMsg(self.pid, ack_type, job_type)
        self._send_msg(conn, msg)

