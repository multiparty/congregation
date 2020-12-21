import os


class NetworkConfig:
    def __init__(self, pid: int, parties: [list, None] = None):
        self.cfg_key = "NETWORK"
        self.pid = pid
        self.parties = parties if parties is not None else []
        self.network_dict = self.set_network_config()

    def set_network_config(self):

        ret = dict()
        ret["pid"] = self.pid
        ret["parties"] = dict()

        for p in self.parties:
            party_data = p.split(":")
            pid = int(party_data[0])
            if pid in ret["parties"]:
                raise Exception(f"PID {p['pid']} already used.")
            ret["parties"][pid] = {}
            ret["parties"][pid]["host"] = party_data[1]
            ret["parties"][pid]["port"] = party_data[2]

        return ret

    @staticmethod
    def from_env():

        pid = int(os.getenv("PID"))
        parties = [p for p in os.getenv("PARTIES").split(",")]
        return NetworkConfig(pid, parties)
