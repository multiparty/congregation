import asyncio
import subprocess
from congregation.dispatch.dispatcher import Dispatcher
from congregation.config import Config
from congregation.job import JiffJob


class JiffDispatcher(Dispatcher):
    def __init__(self, peer, config: Config):
        super().__init__(peer, config)
        self.dispatch_type = "JIFF"

    def dispatch(self, job: JiffJob):

        self.setup_dispatch(job)
        cmd = f"{job.code_dir}/{job.name}/run_client.sh"
        print(f"Running jiff job at {job.code_dir}/{job.name}/party.js")
        subprocess.call(["bash", cmd])

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
        """
        TODO: Need to send AlertMsg to other parties indicating the mismatch
        """
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

    def _send_config(self, other_pid):

        print(f"Sending ConfigMsg to {other_pid}")
        self.peer.send_cfg(other_pid, self.config_to_exchange, "JIFF")

    def send_config(self, pids):

        for other_pid in pids:
            self._send_config(other_pid)

    def send_config_request(self, other_pid):

        print(f"Sending request for Jiff config to {other_pid}")
        self.peer.send_request(other_pid, "CONFIG", "JIFF")

    def send_config_requests(self, pids):

        for other_pid in pids:
            self.send_config_request(other_pid)

    def config_wait_on(self, pids: list, send_fn: callable, cfg_key: str):
        """
        wait for some msg (determined by cfg_key) from a list of other parties
        on timeout, send a request (determined by send_fn) for the msg we're
        seeking to all parties that timed out
        """

        to_wait_on = [self.parties_config[other_pid][cfg_key] for other_pid in pids]
        self.peer.loop.run_until_complete(asyncio.wait(to_wait_on, timeout=30))

        not_done = []
        for other_pid in pids:
            fut = self.parties_config[other_pid][cfg_key]
            if isinstance(fut, asyncio.Future):
                if fut.done():
                    self.parties_config[other_pid][cfg_key] = fut.result()
                else:
                    not_done.append(other_pid)

        if not_done:
            print(f"Timeout while waiting for {cfg_key} from the following parties: {not_done}, trying again.")
            send_fn(not_done)
            self.config_wait_on(not_done, send_fn, cfg_key)

    def exchange_config(self):

        self.send_config([pid for pid in self.parties_config.keys()])
        self.config_wait_on([pid for pid in self.parties_config.keys()], self.send_config_requests, "CFG")
        self.config_wait_on([pid for pid in self.parties_config.keys()], self.send_config, "ACK")

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

    @staticmethod
    def _dispatch_server(job: JiffJob):

        cmd = f"{job.code_dir}/{job.name}/run_server.sh"
        print(f"Dispatching Jiff server for job {job.name}")
        p = subprocess.Popen(["bash", cmd])
        print(f"Jiff server launched with PID {p.pid}")

    def _synchronize_server(self, job: JiffJob):
        """
        TODO timeout stuff
        """

        jc = self.config.system_configs["JIFF_CODEGEN"]
        # if server party isn't a compute party, we assume that
        # the jiff server for this computation is already running
        if jc.server_pid in jc.all_pids:
            if self.pid == jc.server_pid:
                # dispatch server and send ready msgs to other parties
                self._dispatch_server(job)
                for pid in self.parties_ready.keys():
                    print(f"Sending ReadyMsg to {pid}")
                    self.peer.send_ready(pid, "JIFF")
            else:
                # wait for ReadyMsg from server party
                self.peer.loop.run_until_complete(
                    asyncio.wait_for(self.parties_ready[jc.server_pid], timeout=None)
                )

    def synchronize(self, job: JiffJob):

        self._synchronize_config()
        self._synchronize_server(job)
