
import pickle

import pandas as pd
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from utils import connect_ssh_tunnel, connect_to_db, relative_path


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
    df_cours = pd.read_sql('Course',mysqlConn)
    ssh.close()
    df_city = df['city'].unique().tolist()

    df_country = df["country"].unique().tolist()
    df_level_of_education = df["level_of_education"].unique().tolist()
    df_cours = df_cours['id'].unique().tolist()

    context = {'city': df_city,
               "country":df_country,
               "level_of_education":df_level_of_education,
               "course_id":df_cours,}
    
    
    if request.method == 'POST':

        body = request.POST.get('body','')
        gender = request.POST.get('gender')
        year_of_birth = request.POST.get('year_of_birth')
        city = request.POST.get('city','')
        country = request.POST.get('country','')
        level_of_education = request.POST.get('level_of_education')
        nb_messages = request.POST.get('nb_messages')
        course_id = request.POST.get('course_id')
    
        from textblob import Blobber
        from textblob_fr import PatternAnalyzer, PatternTagger

        tb = Blobber(pos_tagger=PatternTagger(), analyzer=PatternAnalyzer())

        print(body)

        blob = tb(body)
        polarity, subjectivity = blob.sentiment
        
        # do something with the input data
        df = pd.DataFrame({
                            'gender' : gender ,
                            'year_of_birth' : year_of_birth,
                            'city' : city,
                            'country' : country,
                            'level_of_education' : level_of_education,
                            "polarity" : polarity,
                            "subjectivity" : subjectivity,
                            "nb_messages" : nb_messages,
                            "course_id" : course_id,
                            
                            }, index=[0])

        print(df)

        model = pickle.load(open(relative_path('GIGAMODEL.model'), 'rb'))

        result = round(model.predict(df)[0], 2)

        context['result'] = result
        print(context['result'])

    return render(request, 'score.html', context)
