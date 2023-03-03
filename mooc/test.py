import json
from sqlalchemy import MetaData
from sqlalchemy.dialects.mysql import insert
from utils import connect_ssh_tunnel, connect_to_db, relative_path

config_file = relative_path("config.yaml")

sshtunnel_mongodb = connect_ssh_tunnel(config_file, "ssh_mongodb")
sshtunnel_mysql = connect_ssh_tunnel(config_file, "ssh_mysql")

mongoClient = connect_to_db(config_file, "database_mongodb")
mysqlEngine = connect_to_db(config_file, "database_mysql")

print(mongoClient)
print(mysqlEngine)

META = MetaData()
META.reflect(bind=mysqlEngine)

TABLE_USERS = META.tables['Users']
TABLE_THREADS = META.tables['Threads']
TABLE_MESSAGES = META.tables['Messages']
TABLE_COURSE = META.tables['Course']
TABLE_NOTES = META.tables['Notes']

db = mongoClient['g1-MOOC']

cursor = db['User'].find()

c = 0
for doc in cursor:

    for key in doc:
        if key in ('_id', 'id', 'username'): continue

        if key == 'data':
            c+= 1
            print(doc['_id'])

print(c)