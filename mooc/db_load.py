import gzip
import time
from math import ceil
from .utils import calc_time, relative_path

import pandas as pd
import sqlalchemy.types as SQLAT
import yaml
from sqlalchemy import Column, ForeignKey, MetaData, Table

import db_connection as db_con


def convert_dtype(types: dict) -> dict:

    conv = {
        'VARCHAR':  SQLAT.VARCHAR,
        'SMALLINT': SQLAT.SMALLINT,
        'BOOLEAN':  SQLAT.BOOLEAN,
        'INTEGER':  SQLAT.INTEGER,
        'FLOAT':    SQLAT.FLOAT
    }

    return {k: conv[v]() if len(v.split()) == 1 else conv[v.split()[0]](int(v.split()[1])) for k, v in types.items()}


##### Chargement des configs
config = yaml.safe_load(open(relative_path('python_files', 'config_db_load.yaml'), 'r'))

param = config['param']

##### Params
ch_size = param['chunksize']
doss = param['folder']

section = param['config_file_section']

meta = MetaData()

##### Connexion bdd
print(f"\nConnexion à '{section}'...")
engine = db_con.create_db(relative_path('python_files', param['connection_config_file']), section, pyodbc=False)

if engine == None:
    quit()

print('\nConnexion établie !', end='\n\n')

##### Noms des Tables + temps
table_names = list(config['files'].keys())
global_time = time.time()

##### Boucle création des tables
print("Création des tables :", end='\n\n')

list_references = []

n_table = 0
for table_name in table_names:

    n_table += 1

    table_config = config['files'][table_name]
    column_types = convert_dtype(table_config['column_types'])
    columns = []

    for column_name in column_types:
        if 'foreign_keys' in table_config and column_name in table_config['foreign_keys']:

            reference = table_config['foreign_keys'][column_name]

            if reference not in list_references:
                list_references.append(reference)

            columns.append(Column(
                column_name,
                column_types[column_name],
                ForeignKey(reference),
                primary_key= column_name in table_config['primary_keys']
            ))
        else:
            columns.append(Column(
                column_name,
                column_types[column_name],
                primary_key= column_name in table_config['primary_keys']
            ))

    Table(
        table_name,
        meta,
        *columns
    )

    print(f"{n_table:2}/{len(table_names)} - {table_name}")

print("\nReferences à vérifier :", ', '.join(list_references), end='\n\n')

meta.create_all(engine)

conn = engine.connect()

##### Boucle remplissage des tables
print("Remplissage des tables :", end='\n\n')

dict_references = {k: [] for k in list_references}

n_table = 0
for table_name in table_names:

    n_table += 1
    table_time = time.time()

    table_config = config['files'][table_name]
    path = relative_path(doss, table_config['name_file'])

    print(f"{n_table:2}/{len(table_names)} - {table_name} - nombre de lines : ", end='')
    nb_lines = sum(1 for _ in gzip.open(path)) - 1
    print(f"{nb_lines:,}")

    nb_chunks = ceil(nb_lines / ch_size)

    column_types = convert_dtype(table_config['column_types'])

    with pd.read_csv(path, sep='\t', na_values=table_config['na_values'], quoting=3, low_memory=False, chunksize=ch_size) as chunk_it:

        n = 0
        for df in chunk_it:

            lines_time = time.time()

            if 'explode_on' in table_config:

                explode_on = table_config['explode_on']

                new_name = explode_on if 'explode_new_name' not in table_config else table_config['explode_new_name']

                df[new_name] = df[explode_on].apply(lambda x: str(x).split(','))

                df = df.explode(new_name).dropna()

            df = df[list(column_types.keys())]

            names_references = [k for k in dict_references.keys() if k.split('.')[0] == table_name]

            if names_references:
                for name_reference in names_references:
                    dict_references[name_reference] += df[name_reference.split('.')[1]].tolist()

            if 'foreign_keys' in table_config:
                for column_name in table_config['foreign_keys']:

                    ok = df[column_name].isin(dict_references[table_config['foreign_keys'][column_name]])

                    df[~ok].to_sql(table_name + '_echecs', conn, dtype = column_types, if_exists='append', index=False)

                    df = df[ok]

            df.to_sql(table_name, conn, dtype = column_types, if_exists='append', index=False)

            n += 1
            print(f" - {n_table:2}/{len(table_names)} - {table_name} : {n:5,} / {nb_chunks:,}  -  {n/nb_chunks:8.3%}  -  {calc_time(lines_time)}")

    print(f" ----- {calc_time(table_time)}", end='\n\n')

##### Commit pour être sûr
conn.commit()

print("Temps total :", calc_time(global_time))