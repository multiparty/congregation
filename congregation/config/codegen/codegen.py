import os


class CodeGenConfig:
    def __init__(
            self,
            workflow_name: [str, None],
            pid: int,
            all_pids: list,
            code_path: [str, None] = None,
            input_path: [str, None] = None,
            output_path: [str, None] = None,
            delimiter: [str, None] = ",",
            use_floats: [bool, None] = None
    ):
        self.cfg_key = "CODEGEN"
        self.workflow_name = workflow_name if workflow_name is not None else "workflow"
        self.pid = pid
        self.all_pids = all_pids
        self.code_path = code_path if code_path is not None else f"/tmp/{self.workflow_name}-code/"
        self.input_path = input_path if input_path is not None else f"/tmp/{self.workflow_name}/"
        self.output_path = output_path if output_path is not None else f"/tmp/{self.workflow_name}/"
        self.delimiter = delimiter if delimiter is not None else ","
        self.use_floats = bool(int(use_floats)) if use_floats is not None else True

    @staticmethod
    def get_values_from_env():

        return [
            os.getenv("WORKFLOW_NAME"),
            int(os.getenv("PID")),
            [int(p) for p in os.getenv("ALL_PIDS").split(",")],
            os.getenv("CODE_PATH"),
            os.getenv("INPUT_PATH"),
            os.getenv("OUTPUT_PATH"),
            os.getenv("DELIMITER"),
            os.getenv("USE_FLOATS")
        ]

    @staticmethod
    def from_env():

        vals = CodeGenConfig.get_values_from_env()
        return CodeGenConfig(*vals)
