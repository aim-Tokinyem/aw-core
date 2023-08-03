from aw_core.config import load_config_toml

default_config = """
[postgreysql]

drivers = ""
host = ""
port = ""
database = ""
username = ""
password = ""

""".strip()


def load_config():
    return load_config_toml("aw-datastore", default_config)
