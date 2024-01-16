from django.shortcuts import render
from datetime import datetime, timedelta
# Create your views to query data from ETLTransaction models. Here is an example:
from app.models import ETLTransaction
def etl_transaction_list(request):
    today_now=   datetime.now()
    today_str = today_now.strftime("%Y-%m-%d")
    next_day_from_now = today_now + timedelta(days=1)
    next_day_str=next_day_from_now.strftime("%Y-%m-%d")
    # filter by condition
    #trans_queryset = ETLTransaction.objects.all()
    x=datetime.strptime(today_str,"%Y-%m-%d")
    y=datetime.strptime(next_day_str,"%Y-%m-%d")
    trans_queryset = ETLTransaction.objects.filter(trans_datetime__gte=x,trans_datetime__lt=y )
    return render(request, 'app/etl_transaction_list.html', {'etl_transaction_list': trans_queryset})
