from django.urls import path
from .views import data_movies,data_additional, data_timeline


urlpatterns = [
    path('', data_movies, name='data_movies'),
    path('additional', data_additional, name='data_additional'),
    path('timeline', data_timeline, name='data_timeline'),
]