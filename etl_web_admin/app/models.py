from django.db import models
from datetime import datetime

# Create your models here.

LOAD_TO_BQ_TYPE = (
    ("merge", "merge"),
    ("bq-storage-api", "bq-storage-api"),
)
class ViewSource(models.Model):
    name = models.CharField(max_length=255)
    load_type =    models.CharField(max_length = 50,choices = LOAD_TO_BQ_TYPE,
        default = '0',verbose_name='CDC-Load To BQ Type'
    )
    app_conten_type_id=models.IntegerField(help_text='id from django_content_type table in your django database app.')
    app_key_name = models.CharField(max_length=255,help_text='key name from view in your django database app.')
    app_changed_field_mapping = models.TextField(help_text="all fields related to columns such as filed1,filed2,filed3.")
    class Meta:
        managed = False
        db_table = 'view_source'
        ordering = ['id']
    def __str__(self):
     return self.name
    
class ETLTransaction(models.Model):

    trans_datetime = models.CharField(max_length=100)
    view_source = models.ForeignKey(ViewSource, on_delete=models.CASCADE)
    no_rows=models.IntegerField()
    is_consistent=models.IntegerField()
    is_complete=models.IntegerField()
    class Meta:
        managed = False
        db_table = 'etl_transaction'
        ordering = ['-id']

    
