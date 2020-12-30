import asyncio
import pickle
from functools import partial
from congregation.config import Config
from congregation.net.messages import *
from congregation.net.protocol import CongregationProtocol
from congregation.net.handler import Handler
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
            lambda: CongregationProtocol(self),
            host=self.host,
            port=self.port
        )
        self.loop.run_until_complete(self.server)
        self.handler = Handler(self)
        self.dispatcher = None

    def register_dispatcher(self, dispatcher: Dispatcher):

        self.dispatcher = dispatcher
        ret = []
        # TODO: might want to match instead on a job_id or something
        for m in self.msg_buffer:
            if m.job_type == dispatcher.dispatch_type:
                self.handler.handle_msg(m)
            else:
                ret.append(m)
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
                        lambda: CongregationProtocol(self),
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

    def _send_msg(self, to_pid, m):

        if to_pid not in self.peer_connections:
            raise Exception(
                f"Can't send {m.msg_type} Msg to party {to_pid}. "
                f"PID not found in peer connections."
            )
        formatted = pickle.dumps(m) + b"\n\n\n"
        self.peer_connections[to_pid].write(formatted)

    def send_iam(self, conn):

        if isinstance(conn, asyncio.Future):
            transport, protocol = conn.result()
        else:
            transport = conn

        m = IAMMsg(self.pid)
        formatted = pickle.dumps(m) + b"\n\n\n"
        transport.write(formatted)

    def send_cfg(self, to_pid, cfg, job_type):

        m = ConfigMsg(self.pid, cfg, job_type)
        self._send_msg(to_pid, m)

    def send_ack(self, to_pid, ack_type, job_type):

        m = AckMsg(self.pid, ack_type, job_type)
        self._send_msg(to_pid, m)
