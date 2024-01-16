from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
import pandas as pd
from   app.models import *
from datetime import datetime, timedelta
def send_email_with_excel_file(email_info):
    from django.core.mail import BadHeaderError
    from django.core.mail import EmailMessage
    try:
        email = EmailMessage(
        email_info['subject'],email_info['message'],
        'smartapp-service@yipintsoigroup.com',email_info['send_to']
    )
        email.send()
    except BadHeaderError as ex:
        raise ex
    return True

class Command(BaseCommand):
    help = 'Collect ETL transactoina dn error to send mail to administrator'
    def handle(self, *args, **options):
        try:
           # get data from transaction and error and write them to dataframe.tohtml as messsage
            today_now=   datetime.now()
            today_str = today_now.strftime("%Y-%m-%d")
            next_day_from_now = today_now + timedelta(days=1)
            next_day_str=next_day_from_now.strftime("%Y-%m-%d")
            # filter by condition
            #trans_queryset = ETLTransaction.objects.all()
            x=datetime.strptime(today_str,"%Y-%m-%d")
            y=datetime.strptime(next_day_str,"%Y-%m-%d")
            trans_queryset = ETLTransaction.objects.filter(trans_datetime__gte=x,trans_datetime__lt=y )
            # dataframe
            transDF = pd.DataFrame.from_records(trans_queryset .values())
            tradsnHTMLContent=transDF.to_html(index=False)

            # send email 
            title=f"CSC PostgresDB-TO-BQ : ETL Transaction on {today_str}"
            message=tradsnHTMLContent
            listRecipients =settings.EMAIL_ADMIN_FOR_MONTHLY_NOTIFICATION
            email_info = {'subject': title, 'message': message, 'send_to': listRecipients}
            is_sussessful = send_email_with_excel_file(email_info)
            
        except Exception as ex:
            # util.add_error_to_file(str(ex))
            print(str(ex))

