from congregation.config.network import NetworkConfig


class CodeGenConfig:
    def __init__(
            self,
            name: str,
            pid: int,
            all_pids: list,
            code_path: [str, None] = None,
            input_path: [str, None] = None,
            output_path: [str, None] = None,
            delimiter: [str, None] = ",",
            use_floats: [bool, None] = True,
            use_leaky_ops: [bool, None] = False
    ):
        self.name = name
        self.pid = pid
        self.all_pids = all_pids
        self.code_path = code_path if code_path is not None else f"/tmp/{name}-code/"
        self.input_path = input_path if input_path is not None else f"/tmp/{name}/"
        self.output_path = output_path if output_path is not None else f"/tmp/{name}/"
        self.delimiter = delimiter
        self.use_floats = use_floats
        self.use_leaky_ops = use_leaky_ops
        self.system_configs = {}

    def add_network_config(self, cfg: NetworkConfig):

        self.system_configs["network"] = cfg
        return self
