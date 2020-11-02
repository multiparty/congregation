from congregation.config.codegen.codegen import CodeGenConfig
import os


class JiffConfig(CodeGenConfig):
    def __init__(
            self,
            workflow_name: str,
            pid: int,
            all_pids: list,
            code_path: [str, None] = None,
            input_path: [str, None] = None,
            output_path: [str, None] = None,
            delimiter: [str, None] = ",",
            use_floats: [bool, None] = True,
            jiff_lib_path: [str, None] = None,
            server_ip: [str, None] = None,
            server_port: [str, int, None] = None,
            server_pid: [int, None] = None
    ):
        super(JiffConfig, self)\
            .__init__(
            workflow_name,
            pid,
            all_pids,
            code_path,
            input_path,
            output_path,
            delimiter,
            use_floats
        )
        self.cfg_key = "JIFF_CODEGEN"
        self.jiff_lib_path = jiff_lib_path
        self.server_ip = server_ip if server_ip is not None else "0.0.0.0"
        self.server_port = server_port if server_port is not None else 9000
        self.server_pid = int(server_pid) if server_ip is not None else None

    @staticmethod
    def get_values_from_env():

        base_vals = CodeGenConfig.get_values_from_env()
        jiff_vals = [
            os.getenv("JIFF_LIB_PATH"),
            os.getenv("SERVER_IP"),
            os.getenv("SERVER_PORT"),
            os.getenv("SERVER_PID")
        ]

        return base_vals + jiff_vals

    @staticmethod
    def from_env():

        vals = JiffConfig.get_values_from_env()
        return JiffConfig(*vals)
