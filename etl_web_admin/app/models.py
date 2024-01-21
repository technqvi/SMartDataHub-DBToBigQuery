from django.db import models
from datetime import datetime

# Create your models here.

LOAD_TO_BQ_TYPE = (
    ("merge", "merge"),
    ("bq-storage-api", "bq-storage-api"),
)
class ViewSource(models.Model):
    name = models.CharField(verbose_name="View Name",max_length=255,unique=True)
    main_source_table_name= models.CharField(verbose_name="Main Table Name",max_length=255,help_text="main table to create view.")
    load_type =    models.CharField(max_length = 50,choices = LOAD_TO_BQ_TYPE,
        default = '0',verbose_name='CDC-Load To BQ Type'
    )
    app_conten_type_id=models.IntegerField(verbose_name="Table Content ID",help_text='id from django_content_type table in your django database app.')
    app_key_name = models.CharField(verbose_name="PK Name",max_length=255,help_text='key name from view in your django database app.')
    app_changed_field_mapping = models.TextField(verbose_name="List Mapping Columns",help_text="all fields related to columns such as filed1,filed2,filed3.")
    app_fk_name_list = models.TextField(verbose_name="List FK Names",help_text="all fields are pk included in view(Converted type ton Int64)", null=True, blank=True)
    app_datetime_field_list=models.TextField(verbose_name="List DateTime,Date Columns",help_text="all fields are date/datetime type included in view (Applied in bq-storage-api)",null=True, blank=True)
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

    
