from congregation.job.job import Job


class PythonJob(Job):
    def __init__(self, name: str, code_dir: str):
        super(PythonJob, self).__init__(name, code_dir)
        self.job_type = "PYTHON"
