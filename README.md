# MOOC_Groupe_1

## Lien du Trello :

https://trello.com/invite/b/L6iCzTaF/ATTI5c70de06232125a8d6f005c8576e0bc5F837A97F/projet-mooc-cam

## Lien du Google Slide :

https://docs.google.com/presentation/d/1YDlEOJ106HcNGw5hT8LLWF3C90Wn8peyCb6ZjnQz1_M/edit?usp=sharing

## Ficher config

**Pour fonctionner la plupart des scripts ont besoin d'un fichier 'config.yaml' dans le dossier 'mooc'.**

**Celui-ci doit être structuré comme ceci :**

database_mongodb:
    user:           
    password:       
    host:           127.0.0.1
    port:           27017
    type:           mongodb
    db_name:        

database_mysql:
    user:           
    password:       
    host:           127.0.0.1
    port:           3306
    type:           mysql
    db_name:        

ssh_mongodb:
    user:           
    password:       
    host:           
    port:           22
    remote_adr:     
    remote_port:    
    local_adr:      127.0.0.1
    local_port:     27017

ssh_mysql:
    user:           
    password:       
    host:           
    port:           22
    remote_adr:     
    remote_port:    
    local_adr:      127.0.0.1
    local_port:     3306
