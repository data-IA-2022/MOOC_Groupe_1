import json

from utils import connect_ssh_tunnel, connect_to_db, relative_path

config_file = relative_path("config.yaml")

# Se connecter à la base de données MongoDB
sshtunnel = connect_ssh_tunnel(config_file, "ssh")
client = connect_to_db(config_file, "database_mongodb")
db = client['MOOC']
collection = db['forum']

# Créer une liste pour stocker les données
data = []

# Boucler sur chaque document de la collection
for document in collection.find()[:10]:
    # Récupérer le nom d'utilisateur et l'ID s'ils existent
    if '_id' in document:
        message_id = str(document['_id'])

        # Récupérer la valeur de la clé 'content' s'il existe
        if 'content' in document:
            content = document['content']

            # Créer un dictionnaire pour stocker les données de chaque document
            document_data = {'message_id': message_id}

            # Ajouter les données de la clé 'content' au dictionnaire
            if 'user_id' in content:
                document_data['user_id'] = content['user_id']

            if 'username' in content:
                document_data['username'] = content['username']

            # Ajouter le dictionnaire à la liste de données
            data.append(document_data)

# Convertir la liste de données en JSON
json_data = json.dumps(data, indent=4)
print(json_data)

sshtunnel.stop()