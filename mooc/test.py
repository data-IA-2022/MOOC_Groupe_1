import json
from sqlalchemy import MetaData
from sqlalchemy.dialects.mysql import insert
from utils import connect_ssh_tunnel, connect_to_db, relative_path

config_file = relative_path("config.yaml")
sshtunnel_mysql = connect_ssh_tunnel(config_file, "ssh_mysql")

mysqlEngine = connect_to_db(config_file, "database_mysql")


META = MetaData()
META.reflect(bind=mysqlEngine)

TABLE_USERS = META.tables['Users']


stmt = insert(TABLE_USERS).values(id = '252415698525636547854125', username = 'Pouet')

print(stmt)

stmt = stmt.on_duplicate_key_update(
    id = stmt.inserted.id,
    status = 'U'
)

print('-------- EXECUTE')
mysqlEngine.execute(stmt)
print('-------- commit')
mysqlEngine.commit()
print('-------- end')