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

cursor = db['Forum'].find()

errors = {
    'anonymous_to_peers_true':0,
    'anonymous_to_peers_false':0,
    'anonymous_to_peers_errors_true':0,
    'anonymous_to_peers_errors_false':0,
          }

def traitement(msg, thread_id, parent_id=None):
    '''
    Effectue un traitement sur l'obj passé (Message)
    :param msg: Message
    :return:
    '''

    dt = msg['created_at']
    dt = f"{dt[:10]} {dt[11:19]}"

    data_insert = {
        'id' : msg['id'],
        'type' : msg['type'],
        'created_at' : dt,
        'user_id' : msg['user_id'] if 'user_id' in msg else None,
        'depth' : msg['depth'] if 'depth' in msg else None,
        'body' : msg['body'],
        'parent_id' : parent_id,
        'thread_id' : thread_id,
    }

    try:
        if not msg['anonymous'] and not msg['anonymous_to_peers']:
            msg['user_id']
            msg['username']

    except KeyError:
        #print(msg)
        if msg['anonymous_to_peers']:
            errors['anonymous_to_peers_errors_true'] += 1
        else:
            errors['anonymous_to_peers_errors_false'] += 1

        print(thread_id)
        print(parent_id)

    else:

        if 'anonymous_to_peers' in msg:
            if msg['anonymous_to_peers']:
                errors['anonymous_to_peers_true'] += 1
            else:
                errors['anonymous_to_peers_false'] += 1


def recur_message(msg, thread_id, parent_id = None):

    traitement(msg, thread_id, parent_id)
    if 'children' in msg:
        for child in msg['children']:
            recur_message(child, thread_id, parent_id = msg['id'])

    if 'non_endorsed_responses' in msg:
        for child in msg['non_endorsed_responses']:
            recur_message(child, thread_id, parent_id = msg['id'])

    if 'endorsed_responses' in msg:
        for child in msg['endorsed_responses']:
            recur_message(child, thread_id, parent_id = msg['id'])


for doc in cursor:

    recur_message(doc['content'], doc['_id'])

print(errors)