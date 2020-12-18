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
            server_pid: [int, None] = None,
            zp: [int, None] = None,
            extensions: [dict, None] = None
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
        self.server_pid = None if server_pid is None else int(server_pid)
        self.zp = 16777729 if zp is None else int(zp)
        self.extensions = extensions if extensions is not None else self._get_default_extension_data()

    @staticmethod
    def _get_default_extension_data():
        return {
            "fixed_point": {
                "use": False,
                "decimal_digits": 1,
                "integer_digits": 1
            },
            "negative_number": {
                "use": False
            },
            "big_number": {
                "use": False
            }
        }

    @staticmethod
    def _get_extension_data_from_env():

        ret = {
            "fixed_point": {
                "use": os.getenv("FIXED_POINT_USE"),
                "decimal_digits": int(os.getenv("FIXED_POINT_DECIMAL_DIGITS")),
                "integer_digits": int(os.getenv("FIXED_POINT_INTEGER_DIGITS"))
            },
            "negative_number": {
                "use": os.getenv("NEGATIVE_NUMBER_USE")
            },
            "big_number": {
                "use": os.getenv("BIG_NUMBER_USE")
            }
        }

        return ret

    @staticmethod
    def get_values_from_env():

        base_vals = CodeGenConfig.get_values_from_env()
        jiff_vals = [
            os.getenv("JIFF_LIB_PATH"),
            os.getenv("SERVER_IP"),
            os.getenv("SERVER_PORT"),
            os.getenv("SERVER_PID"),
            os.getenv("ZP")
        ]

        return base_vals + jiff_vals

    @staticmethod
    def from_env():

        vals = JiffConfig.get_values_from_env()
        return JiffConfig(*vals)

    @staticmethod
    def from_base_config(c: CodeGenConfig, args: [dict, None]):

        if args is None:
            args = {}

        return JiffConfig(
            c.workflow_name,
            c.pid,
            c.all_pids,
            c.code_path,
            c.input_path,
            c.output_path,
            c.delimiter,
            c.use_floats,
            args.get("jiff_lib_path"),
            args.get("server_ip"),
            args.get("server_port"),
            args.get("server_pid"),
            args.get("zp"),
            args.get("extensions")
        )
