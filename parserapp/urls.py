from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('correlation/', views.correlation_view, name='correlation'),
]
