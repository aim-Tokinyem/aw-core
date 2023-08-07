from aw_core.config import load_config_toml

default_config = """
[postgreysql]

drivers = ""
host = "127.0.0.1"
port = "5432"
database = "postgres"
username = "postgres"
password = "postgres"

""".strip()


def load_config():
    return load_config_toml("aw-datastore", default_config)
