import asyncio
from congregation.config import Config, JiffConfig, CodeGenConfig, NetworkConfig
from congregation.dag import Dag
from congregation.comp import compile_dag
from congregation.part import HeuristicPart
from congregation.codegen.jiff import JiffCodeGen
from congregation.codegen.python import PythonCodeGen
from congregation.net import Peer


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
                cfg["general"]["all_parties"],
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
                    "server_pid": cfg["jiff"]["server_port"],
                    "zp": cfg["jiff"]["zp"] if "zp" in cfg["jiff"] else None,
                    "extensions": cfg["jiff"]["extensions"] if "extensions" in cfg["jiff"] else None
                }
            )
        else:
            jiff_cfg = JiffConfig.from_env()
        self.config.add_config(jiff_cfg)

        return self

    def setup_config(self, cfg: [dict, None] = None):
        """
        Wraps all config setup methods
        """
        self.configure_network(cfg)
        self.configure_codegen(cfg)
        self.configure_jiff(cfg)

        return self

    @staticmethod
    def compile(protocol: callable):

        dag = Dag(protocol())
        compile_dag(dag)
        return dag

    @staticmethod
    def partition(dag: Dag, iteration_limit: [int, None] = 100):

        p = HeuristicPart(dag)
        return p.partition(iteration_limit)

    def generate_code(self, parts: list):
        """
        Generate code from partitions list and write to disk
        """
        for i in range(len(parts)):
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

    def generate(self, protocol: callable, iteration_limit: [int, None] = 100):

        dag = self.compile(protocol)
        parts = self.partition(dag, iteration_limit)
        self.generate_code(parts)
        job_queue = self.generate_jobs(parts)

        return job_queue

    def setup_peer(self):

        loop = asyncio.get_event_loop()
        peer = Peer(loop, self.config)
        peer.connect_to_others()

        return peer

    def dispatch_jobs(self, job_queue, networked_peer):
        return

    def generate_and_dispatch(self, protocol: callable, cfg: [dict, None] = None, iteration_limit: [int, None] = 100):

        self.setup_config(cfg)
        peer = self.setup_peer()
        job_queue = self.generate(protocol, iteration_limit)
        self.dispatch_jobs(job_queue, peer)


