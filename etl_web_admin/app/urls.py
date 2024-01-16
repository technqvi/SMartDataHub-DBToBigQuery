from django.urls import path
from django.conf import settings
from django.conf.urls.static import static

from . import views
urlpatterns = [

    path('transaction/', views.etl_transaction_list, name='list_transaction')

]