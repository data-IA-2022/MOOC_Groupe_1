import time

from sqlalchemy import MetaData
from sqlalchemy.dialects.mysql import insert
from utils import connect_ssh_tunnel, connect_to_db, relative_path, calc_time

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

stmt = []
stmt_notes = []

def recur_message(msg, f, thread_id, parent_id = None):
    '''
    Cette fonction fait un traitement messages de l'objet JSON passé
    :param obj: objet JSON contiens un MESSAGE
    :param f: fonctions à appeler
    :return:
    '''
    #print("Recurse ", obj['id'], obj['depth'] if 'depth' in obj else '-')

    f(msg, thread_id, parent_id)

    if 'children' in msg:
        for child in msg['children']:
            recur_message(child, f, thread_id, parent_id = msg['id'])

    if 'non_endorsed_responses' in msg:
        for child in msg['non_endorsed_responses']:
            recur_message(child, f, thread_id, parent_id = msg['id'])

    if 'endorsed_responses' in msg:
        for child in msg['endorsed_responses']:
            recur_message(child, f, thread_id, parent_id = msg['id'])


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

    if not msg['anonymous'] and not msg['anonymous_to_peers']:
        stmt.append(insert(TABLE_USERS).values(id = msg['user_id'], username = msg['username']))

    stmt.append(insert(TABLE_MESSAGES).values(**data_insert))

    # mysqlEngine.execute("""INSERT INTO Messages
    #                     (id, type, created_at, user_id, depth, body, parent_id) 
    #                     VALUES (%s,%s,%s,%s,%s,%s,%s)
    #                     ON DUPLICATE KEY UPDATE parent_id=VALUES(parent_id), depth=VALUES(depth);""",
    #                     [msg['id'], msg['type'], dt, user_id, msg['depth'] if 'depth' in msg else None, msg['body'], parent_id])


db = mongoClient['g1-MOOC']
start_time = time.time()

print("\n ----- USER\n")

cursor = db['User'].find()
doc_count = len(list(cursor.clone()))
n = 0
perc = 0
len_stmt = len(stmt) + len(stmt_notes)
user_time = time.time()

for doc in cursor:

    gender = None
    year_of_birth = None

    for key in doc:
        if key in ('_id', 'id', 'username', 'data'): continue

        stmt.append(insert(TABLE_COURSE).values(id = key))

        if 'grade' in doc[key]:
            stmt_notes.append(insert(TABLE_NOTES).values(course_id = key, user_id = doc['_id'], grade = doc[key]['grade']))

        if 'gender' in doc[key]:
            gender = doc[key]['gender'] if doc[key]['gender'] != 'None' else None

        if 'year_of_birth' in doc[key]:
            year_of_birth = doc[key]['year_of_birth'] if doc[key]['year_of_birth'] != 'None' else None

    stmt.append(insert(TABLE_USERS).values(id = doc['_id'], username = doc['username'], gender = gender, year_of_birth = year_of_birth))

    n += 1
    n_perc = int(n / doc_count * 100)
    if perc // 5 != n_perc // 5:
        perc = n_perc
        print(f" - {perc:3}%   USER ({len(stmt) + len(stmt_notes) - len_stmt} inserts)")
        len_stmt = len(stmt) + len(stmt_notes)

print(f"--- USER TIME : {calc_time(user_time)}")

print("\n ----- FORUM\n")
cursor = db['Forum'].find()
doc_count = len(list(cursor.clone()))
n = 0
perc = 0
len_stmt = len(stmt) + len(stmt_notes)
forum_time = time.time()

for doc in cursor:

    stmt.append(insert(TABLE_COURSE).values(id = doc['content']['course_id']))
    stmt.append(insert(TABLE_THREADS).values(
            id = doc['_id'],
            course_id = doc['content']['course_id'],
            title = doc['content']['title'],
            comments_count = doc['content']['comments_count']
        ))

    recur_message(doc['content'], traitement, doc['_id'])

    n += 1
    n_perc = int(n / doc_count * 100)
    if perc // 5 != n_perc // 5:
        perc = n_perc
        print(f" - {perc:3}%   FORUM ({len(stmt) + len(stmt_notes) - len_stmt} inserts)")
        len_stmt = len(stmt) + len(stmt_notes)

print(f"--- FORUM TIME : {calc_time(forum_time)}")

print("\n ----- EXECUTE STMT\n")
n = 0
perc = 0
stmt_time = time.time()

for s in stmt:

    s = s.on_duplicate_key_update(
        id = s.inserted.id,
        status = 'U'
    )
    mysqlEngine.execute(s)

    n += 1
    n_perc = int(n / len(stmt) * 100)
    if perc // 5 != n_perc // 5:
        perc = n_perc
        print(f" - {perc:3}%   EXECUTE STMT")

print(f"--- EXECUTE STMT TIME : {calc_time(stmt_time)}")

print("\n ----- EXECUTE STMT NOTES\n")
n = 0
perc = 0
stmt_notes_time = time.time()

for s in stmt_notes:

    mysqlEngine.execute(s)

    n += 1
    n_perc = int(n / len(stmt_notes) * 100)
    if perc // 5 != n_perc // 5:
        perc = n_perc
        print(f" - {perc:3}%   EXECUTE STMT NOTES")

print(f"--- EXECUTE STMT NOTES TIME : {calc_time(stmt_notes_time)}")

print('\n-------- COMMIT')
mysqlEngine.commit()
print('\n-------- END')

print(f"\nTOTAL INSERT COUNT : {len(stmt) + len(stmt_notes)}\n")

print(f"\nGLOBAL TIME : {calc_time(start_time)}")