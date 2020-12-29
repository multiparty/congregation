import asyncio
from subprocess import call
from congregation.dispatch.dispatcher import Dispatcher
from congregation.config import Config
from congregation.job import JiffJob


class JiffDispatcher(Dispatcher):
    def __init__(self, peer, config: Config):
        super().__init__(peer, config)
        self.dispatch_type = "JIFF"

    def dispatch(self, job: JiffJob):

        super().dispatch(job)
        self.synchronize()
        cmd = f"{job.code_dir}/{job.name}/party.js"
        print(f"Running jiff job at {job.code_dir}/{job.name}/party.js")
        call(["node", cmd])

    def setup_config(self):

        jiff_cfg = self.config.system_configs["JIFF_CODEGEN"]
        cfg = {
            "server_ip": jiff_cfg.server_ip,
            "server_pid": jiff_cfg.server_pid,
            "all_pids": jiff_cfg.all_pids,
            "zp": jiff_cfg.zp,
            "extensions": jiff_cfg.extensions
        }
        return cfg

    def _mismatch_alert(self, pid: int, k: str, v: [str, int, list], other_v: [str, int, list]):
        raise Exception(
            f"Jiff config mismatch between self and other party for key {k}\n"
            f"Own value: {v}\nValue for party {pid}: {other_v}"
        )

    def _compare_extensions(self, other_ext: dict, other_pid: int):

        ext = self.config.system_configs["JIFF_CODEGEN"].extensions
        if other_ext["fixed_point"].get("use") != ext["fixed_point"].get("use"):
            self._mismatch_alert(
                other_pid,
                "fixed_point.use",
                ext["fixed_point"].get("use"),
                other_ext["fixed_point"].get("use")
            )
        if other_ext["fixed_point"].get("decimal_digits") != ext["fixed_point"].get("decimal_digits"):
            self._mismatch_alert(
                other_pid,
                "fixed_point.decimal_digits",
                ext["fixed_point"].get("decimal_digits"),
                other_ext["fixed_point"].get("decimal_digits")
            )
        if other_ext["fixed_point"].get("integer_digits") != ext["fixed_point"].get("integer_digits"):
            self._mismatch_alert(
                other_pid,
                "fixed_point.integer_digits",
                ext["fixed_point"].get("integer_digits"),
                other_ext["fixed_point"].get("integer_digits")
            )
        if other_ext["negative_number"].get("use") != ext["negative_number"].get("use"):
            self._mismatch_alert(
                other_pid,
                "negative_number.use",
                ext["negative_number"].get("use"),
                other_ext["negative_number"].get("use")
            )
        if other_ext["big_number"].get("use") != ext["big_number"].get("use"):
            self._mismatch_alert(
                other_pid,
                "big_number.use",
                ext["big_number"].get("use"),
                other_ext["big_number"].get("use")
            )

    def _compare_against_other_config(self, other_cfg: dict, other_pid: int):

        jiff_cfg = self.config.system_configs["JIFF_CODEGEN"]
        if other_cfg["server_ip"] != jiff_cfg.server_ip:
            self._mismatch_alert(other_pid, "server_ip", jiff_cfg.server_ip, other_cfg["server_ip"])
        if other_cfg["server_pid"] != jiff_cfg.server_pid:
            self._mismatch_alert(other_pid, "server_pid", jiff_cfg.server_pid, other_cfg["server_pid"])
        if other_cfg["all_pids"] != jiff_cfg.all_pids:
            self._mismatch_alert(other_pid, "all_pids", jiff_cfg.all_pids, other_cfg["all_pids"])
        if other_cfg["zp"] != jiff_cfg.zp:
            self._mismatch_alert(other_pid, "zp", jiff_cfg.zp, other_cfg["zp"])
        self._compare_extensions(other_cfg.get("extensions"), other_pid)

    def exchange_config(self):

        to_wait_on = []
        for pid in self.parties_config.keys():
            if pid < self.pid:
                print(f"Sending ConfigMsg to {pid}")
                self.peer.send_cfg(
                    self.peer.peer_connections[pid],
                    self.config_to_exchange,
                    "JIFF"
                )
                to_wait_on.append(self.parties_config[pid]["CFG"])
            elif pid > self.pid:
                print(f"Waiting for ConfigMsg from {pid}")
                to_wait_on.append(self.parties_config[pid]["CFG"])
            else:
                pass
        self.peer.loop.run_until_complete(asyncio.gather(*to_wait_on))

        for pid in self.parties_config.keys():
            if pid != self.pid:
                completed_future = self.parties_config[pid]["CFG"]
                self.parties_config[pid]["CFG"] = completed_future.result()

        self._wait_on_acks()

    def _wait_on_acks(self):

        to_wait_on = []
        for pid in self.parties_config.keys():
            if pid != self.pid:
                to_wait_on.append(self.parties_config[pid]["ACK"])

        self.peer.loop.run_until_complete(asyncio.gather(*to_wait_on))
        for pid in self.parties_config.keys():
            if pid != self.pid:
                completed_future = self.parties_config[pid]["ACK"]
                self.parties_config[pid]["ACK"] = completed_future.result()

    def _verify_config_against_others(self):

        for pid in self.parties_config.keys():
            self._compare_against_other_config(self.parties_config[pid]["CFG"], pid)

    def _synchronize_config(self):
        """
        send jiff config to other parties and wait to receive
        their config. Once config messages are received, make
        sure they're all equivalent before proceeding.
        """
        self.exchange_config()
        self._verify_config_against_others()

    def synchronize(self):
        """
        if server_pid in all_pids, party who is server launches
        server and sends ready to all other parties
        then dispatch party.js
        """
        self._synchronize_config()
