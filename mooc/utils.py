from os.path import join, dirname
import time
import yaml
import paramiko
from sshtunnel import SSHTunnelForwarder


def relative_path(*path):

    return join(dirname(__file__), *path)


def calc_time(start_time):

    d = time.time() - start_time
    h = int(d / 3600)
    h = f"{h} h " if d > 3600 else ''
    m = int(d % 3600 / 60)
    m = f"{m} m " if d > 60 else ''
    s = int(d % 3600 % 60)
    s = f"{s} s"
    return h + m + s


def connect_to_db(config_file, section, ssh=False, ssh_section=None, local_port=None):

    # Read configuration information from file
    config = yaml.safe_load(open(config_file, 'r'))

    if ssh:
        ssh_config = config[ssh_section]

        server = SSHTunnelForwarder(
            ssh_config['host'],
            ssh_username=ssh_config['user'],
            ssh_password=ssh_config['password'],
            remote_bind_address=(ssh_config['remote_adr'], ssh_config['remote_port']),
            local_bind_address=(ssh_config['local_adr'], ssh_config['local_port'])
        )

        server.start()

    # Connect to database using SQLAlchemy

    url_pswd = ":{password}".format(**config[section]) if config[section]["password"] != None else ""
    url_user = "{user}{url_pswd}@".format(**config[section], url_pswd = url_pswd) if config[section]["user"] != None else ""
    url_port = ":{port}".format(**config[section]) if config[section]["type"] != "mongodb" else ""
    url_data = "/{db_name}".format(**config[section]) if config[section]["db_name"] != None else ""

    url = "{type}://{url_user}{host}{url_port}{url_data}".format(**config[section], url_user=url_user, url_port=url_port, url_data=url_data)

    if config[section]["type"] == "mongodb":

        from pymongo import MongoClient
        from pymongoarrow.monkey import patch_all

        patch_all()

        return MongoClient(url, config[section]["port"])

    else:

        from sqlalchemy import create_engine

        engine = create_engine(url)

        return engine.connect()