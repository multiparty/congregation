from congregation.config.network import NetworkConfig
from congregation.config.codegen import CodeGenConfig, JiffConfig


class Config:
    def __init__(self):
        self.system_configs = {}

    def add_config(self, cfg: [NetworkConfig, CodeGenConfig, JiffConfig]):

        self.system_configs[cfg.cfg_key] = cfg
        return self

    @staticmethod
    def from_env():

        network_conf = NetworkConfig.from_env()
        codegen_conf = CodeGenConfig.from_env()
        jiff_conf = JiffConfig.from_env()

        conf = Config()
        conf.add_config(network_conf)
        conf.add_config(codegen_conf)
        conf.add_config(jiff_conf)

        return conf
