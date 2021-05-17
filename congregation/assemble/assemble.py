import asyncio
import json
from congregation.codegen import JiffCodeGen, PythonCodeGen
from congregation.comp import compile_dag, compile_dag_without_optimizations
from congregation.config import Config, JiffConfig, CodeGenConfig, NetworkConfig
from congregation.dag import Dag
from congregation.dispatch import JiffDispatcher, PythonDispatcher
from congregation.net import Peer
from congregation.part import HeuristicPart


class Assemble:
    def __init__(self):
        self.config = Config()

    def configure_network(self, cfg: [dict, None] = None):

        if cfg is not None:
            net_cfg = NetworkConfig(
                cfg["general"]["pid"],
                cfg["network"]["parties"]
            )
        else:
            net_cfg = NetworkConfig.from_env()
        self.config.add_config(net_cfg)

        return self

    def configure_codegen(self, cfg: [dict, None] = None):

        if cfg is not None:
            cg_cfg = CodeGenConfig(
                cfg["general"]["workflow_name"] if "workflow_name" in cfg["general"] else "congregation-workflow",
                cfg["general"]["pid"],
                cfg["general"]["all_pids"],
                cfg["general"]["code_path"] if "code_path" in cfg["general"] else "/tmp/",
                cfg["general"]["data_path"],
                cfg["general"]["data_path"],
                cfg["general"]["delimiter"] if "delimiter" in cfg["general"] else ",",
                cfg["general"]["use_floats"] if "use_floats" in cfg["general"] else False
            )
        else:
            cg_cfg = CodeGenConfig.from_env()
        self.config.add_config(cg_cfg)

        return self

    def configure_jiff(self, cfg: [dict, None] = None):

        if cfg is not None:
            if "CODEGEN" not in self.config.system_configs:
                self.configure_codegen(cfg)
            jiff_cfg = JiffConfig.from_base_config(
                self.config.system_configs["CODEGEN"],
                {
                    "jiff_lib_path": cfg["jiff"]["jiff_lib_path"],
                    "server_ip": cfg["jiff"]["server_ip"],
                    "server_port": cfg["jiff"]["server_port"],
                    "server_pid": cfg["jiff"]["server_pid"],
                    "zp": cfg["jiff"]["zp"] if "zp" in cfg["jiff"] else None,
                    "extensions": cfg["jiff"]["extensions"] if "extensions" in cfg["jiff"] else None
                }
            )
        else:
            jiff_cfg = JiffConfig.from_env()
        self.config.add_config(jiff_cfg)

        return self

    def setup_config(self, cfg: [dict, str, None] = None):
        """
        Wraps all config setup methods
        """
        if isinstance(cfg, str):
            print(f"Loading config from file: {cfg}")
            cfg = json.loads(open(cfg, "r").read())
        self.configure_network(cfg)
        self.configure_codegen(cfg)
        self.configure_jiff(cfg)

        return self

    @staticmethod
    def construct_dag(roots: set):
        return Dag(roots)

    @staticmethod
    def compile(protocol: callable, enable_optimizations: [bool, None] = True):

        dag = Dag(protocol())
        if enable_optimizations:
            compile_dag(dag)
        else:
            compile_dag_without_optimizations(dag)

        return dag

    @staticmethod
    def partition(dag: Dag, iteration_limit: [int, None] = 100):

        p = HeuristicPart(dag)
        return p.partition(iteration_limit)

    def _involves_this_party(self, d: Dag):
        """
        Look at input DAG and see if it involves data that we're holding
        """
        for r in d.roots:
            for ps in r.out_rel.stored_with:
                if self.config.system_configs['CODEGEN'].pid in ps:
                    return True
        return False

    def generate_code(self, parts: list):
        """
        Generate code from partitions list and write to disk
        """
        for i in range(len(parts)):

            if not self._involves_this_party(parts[i][0]):
                # not our data, skip job
                continue

            if parts[i][1] == "python":
                cg = PythonCodeGen(
                    self.config,
                    parts[i][0],
                    f"{self.config.system_configs['CODEGEN'].workflow_name}-python-job-{i}"
                )
                cg.generate()
            elif parts[i][1] == "jiff":
                cg = JiffCodeGen(
                    self.config,
                    parts[i][0],
                    f"{self.config.system_configs['CODEGEN'].workflow_name}-jiff-job-{i}"
                )
                cg.generate()
            else:
                raise Exception(f"Unrecognized backend from partition: {parts[i][1]}.")

    def generate_jobs(self, parts: list):
        """
        Generate and return jobs from partitions list
        """
        ret = []
        for i in range(len(parts)):

            if not self._involves_this_party(parts[i][0]):
                # not our data, skip job
                continue

            if parts[i][1] == "python":
                cg = PythonCodeGen(
                    self.config,
                    parts[i][0],
                    f"{self.config.system_configs['CODEGEN'].workflow_name}-python-job-{i}"
                )
                ret.append(cg.generate_job())
            elif parts[i][1] == "jiff":
                cg = JiffCodeGen(
                    self.config,
                    parts[i][0],
                    f"{self.config.system_configs['CODEGEN'].workflow_name}-jiff-job-{i}"
                )
                ret.append(cg.generate_job())
            else:
                raise Exception(f"Unrecognized backend from partition: {parts[i][1]}.")

        return ret

    def generate(
            self,
            protocol: callable,
            iteration_limit: [int, None] = 100,
            enable_optimizations: [bool, None] = True
    ):

        dag = self.compile(protocol, enable_optimizations)
        parts = self.partition(dag, iteration_limit)
        self.generate_code(parts)
        job_queue = self.generate_jobs(parts)

        return job_queue

    def setup_peer(self):

        loop = asyncio.get_event_loop()
        peer = Peer(loop, self.config)
        peer.connect_to_others()

        return peer

    def dispatch_jobs(self, job_queue: list, networked_peer: Peer):

        dispatchers = {
            "JIFF": JiffDispatcher,
            "PYTHON": PythonDispatcher
        }

        for j in job_queue:
            try:
                dispatcher = dispatchers[j.job_type](networked_peer, self.config)
                dispatcher.dispatch(j)
            except KeyError:
                print(f"No dispatcher found for job type {j.job_type}")

    def generate_and_dispatch(
            self,
            protocol: callable,
            cfg: [dict, str, None] = None,
            iteration_limit: [int, None] = 100,
            enable_optimizations: [bool, None] = True
    ):

        self.setup_config(cfg)
        peer = self.setup_peer()
        job_queue = self.generate(protocol, iteration_limit, enable_optimizations)
        self.dispatch_jobs(job_queue, peer)
