
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt


def home(request):
    return render(request, 'home.html')

def analyses(request):
    return render(request, 'analyses.html')


