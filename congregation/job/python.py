from congregation.job.base import Job


class PythonJob(Job):
    def __init__(self, name: str, code_dir: str):
        super(PythonJob, self).__init__(name, code_dir)
