import time

from sqlalchemy import MetaData
from sqlalchemy.dialects.mysql import insert
from utils import calc_time, connect_ssh_tunnel, connect_to_db, relative_path

config_file = relative_path("config.yaml")

sshtunnel_mongodb = connect_ssh_tunnel(config_file, "ssh_mongodb")
sshtunnel_mysql = connect_ssh_tunnel(config_file, "ssh_mysql")

mongoClient = connect_to_db(config_file, "database_mongodb")
mysqlEngine = connect_to_db(config_file, "database_mysql")

mysqlConn = mysqlEngine.connect().execution_options(isolation_level="AUTOCOMMIT")
mysqlConn.begin()

print(mongoClient)
print(mysqlEngine)

META = MetaData()
META.reflect(bind=mysqlEngine)

TABLE_USERS = META.tables['Users']
TABLE_THREADS = META.tables['Threads']
TABLE_MESSAGES = META.tables['Messages']
TABLE_COURSE = META.tables['Course']
TABLE_NOTES = META.tables['Notes']

stmts = {
    'users': [],
    'threads': [],
    'messages': [],
    'course': [],
    'notes': [],
}


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
        stmts['users'].append(insert(TABLE_USERS).values(id = msg['user_id'], username = msg['username']).prefix_with('IGNORE'))

    stmts['messages'].append(insert(TABLE_MESSAGES).values(**data_insert).prefix_with('IGNORE'))

    # mysqlEngine.execute("""INSERT INTO Messages
    #                     (id, type, created_at, user_id, depth, body, parent_id) 
    #                     VALUES (%s,%s,%s,%s,%s,%s,%s)
    #                     ON DUPLICATE KEY UPDATE parent_id=VALUES(parent_id), depth=VALUES(depth);""",
    #                     [msg['id'], msg['type'], dt, user_id, msg['depth'] if 'depth' in msg else None, msg['body'], parent_id])


def traitement_user(doc):

    gender = None
    year_of_birth = None

    for key in doc:
        if key in ('_id', 'id', 'username', 'data'): continue

        stmts['course'].append(insert(TABLE_COURSE).values(id = key).prefix_with('IGNORE'))

        if 'grade' in doc[key]:
            stmts['notes'].append(insert(TABLE_NOTES).values(course_id = key, user_id = doc['_id'], grade = doc[key]['grade']).prefix_with('IGNORE'))

        if 'gender' in doc[key]:
            gender = doc[key]['gender'] if doc[key]['gender'] != 'None' else None

        if 'year_of_birth' in doc[key]:
            year_of_birth = doc[key]['year_of_birth'] if doc[key]['year_of_birth'] != 'None' else None

    stmts['users'].append(insert(TABLE_USERS).values(id = doc['_id'], username = doc['username'], gender = gender, year_of_birth = year_of_birth).prefix_with('IGNORE'))


def traitement_forum(doc):

    stmts['course'].append(insert(TABLE_COURSE).values(id = doc['content']['course_id']).prefix_with('IGNORE'))
    stmts['threads'].append(insert(TABLE_THREADS).values(
            id = doc['_id'],
            course_id = doc['content']['course_id'],
            title = doc['content']['title'],
            comments_count = doc['content']['comments_count']
        ).prefix_with('IGNORE'))

    recur_message(doc['content'], traitement, doc['_id'])


def boucles(cursor, boucle, name_print, len_cursor, print_insert = False):

    print(f"\n ----- {name_print}\n")

    n = 0
    perc = 0
    if print_insert :
        len_stmt = sum([len(stmts[key]) for key in stmts])
    total_time = time.time()
    t = time.time()

    for doc in cursor:

        boucle(doc)

        n += 1
        n_perc = int(n / len_cursor * 100)

        if perc // 5 != n_perc // 5:

            perc = n_perc

            str_insert = f"({sum([len(stmts[key]) for key in stmts]) - len_stmt:7} inserts) " if print_insert else ""
            print(f" - {perc:3} %   {name_print:10} {str_insert}  {calc_time(t)}")

            t = time.time()

            if print_insert:
                len_stmt = sum([len(stmts[key]) for key in stmts])

    print(f"--- {name_print} TIME : {calc_time(total_time)}")


db = mongoClient['g1-MOOC']
global_time = time.time()

cursor = db['User'].find().limit(100)
len_cursor = len(list(cursor.clone()))

boucles(cursor, traitement_user, "USER", len_cursor, True)

cursor = db['Forum'].find().limit(100)
len_cursor = len(list(cursor.clone()))

boucles(cursor, traitement_forum, "FORUM", len_cursor, True)

tables_in_order = ['course', 'users', 'notes', 'threads', 'messages']

for key in tables_in_order:
    boucles(stmts[key], mysqlConn.execute, f"EXECUTE {key.upper()}", len(stmts[key]))

print('\n-------- COMMIT')
mysqlConn.commit()
print('\n-------- END')

print(f"\nTOTAL INSERT COUNT : {sum([len(stmts[key]) for key in stmts])}\n")

print(f"\nGLOBAL TIME : {calc_time(global_time)}")