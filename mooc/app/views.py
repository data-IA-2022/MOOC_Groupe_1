
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt


from utils import connect_ssh_tunnel, connect_to_db, relative_path
import pandas as pd


def home(request):
    return render(request, 'home.html')

def analyses(request):
    return render(request, 'analyses.html')


def detection(request):
    return render(request, 'detection.html')


def sentiments(request):
    return render(request, 'sentiments.html')

def score(request):
    path_config = relative_path("config.yaml")

    ssh = connect_ssh_tunnel(path_config, 'ssh_mysql')

    mysqlEngine = connect_to_db(path_config, 'database_mysql')
    mysqlConn = mysqlEngine.connect()
    mysqlConn.begin()

    df = pd.read_sql('Users', mysqlConn)
    ssh.close()
    df_city = df['city'].unique().tolist()

    df_country = df["country"].unique().tolist()
    df_level_of_education = df["level_of_education"].unique().tolist()
   

    context = {'city': df_city,
               "country":df_country,
               "level_of_education":df_level_of_education}
    return render(request, 'score.html', context)
