from congregation.job.base import Job


class JiffJob(Job):
    def __init__(self, name: str, code_dir: str):
        super(JiffJob, self).__init__(name, code_dir)
