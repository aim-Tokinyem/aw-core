from aw_core.config import load_config_toml

default_config = """
[postgreysql]

drivers = ""
host = "localhost"
port = "5432"
database = "ACTIVITY_POSTGREY_V2"
username = "postgres"
password = "P@ssw0rd"

""".strip()


def load_config():
    return load_config_toml("aw-datastore", default_config)
