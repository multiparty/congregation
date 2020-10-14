

class NetworkConfig:
    def __init__(self, pid: int, parties: [list, None] = None):
        self.pid = pid
        self.parties = parties if parties is not None else []
        self.network_dict = self.set_network_config()

    def add_party(self, party: dict):
        self.parties.append(party)
        self.network_dict = self.set_network_config()
        return self

    def set_network_config(self):

        ret = dict()
        ret["pid"] = self.pid
        ret["parties"] = dict()
        for p in self.parties:
            if p["pid"] in ret:
                raise Exception(
                    f"Each party must have a unique PID. \n"
                    f"The following party configuration is invalid: {self.parties}"
                )
            ret["parties"][p["pid"]] = {}
            ret["parties"][p["pid"]]["host"] = p["host"]
            ret["parties"][p["pid"]]["port"] = p["port"]

        return ret
