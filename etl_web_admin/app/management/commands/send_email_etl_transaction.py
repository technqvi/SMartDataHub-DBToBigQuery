from django.core.management.base import BaseCommand, CommandError
from django.conf import settings

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
            

            # send email 
            title="DB-TO-BQ : Demo send email ETL Transaction"
            message="Test send email ETL Transaction"
            listRecipients =settings.EMAIL_ADMIN_FOR_MONTHLY_NOTIFICATION
            email_info = {'subject': title, 'message': message, 'send_to': listRecipients}
            is_sussessful = send_email_with_excel_file(email_info)
            
        except Exception as ex:
            # util.add_error_to_file(str(ex))
            print(str(ex))

