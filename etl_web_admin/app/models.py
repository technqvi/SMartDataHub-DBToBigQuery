from django.db import models
from datetime import datetime

# Create your models here.

class ViewSource(models.Model):
    name = models.CharField(max_length=255)
    class Meta:
        managed = False
        db_table = 'view_source'
        ordering = ['id']
    def __str__(self):
     return self.name
    
class ETLTransaction(models.Model):

    trans_datetime = models.CharField(max_length=100)
    view_source = models.ForeignKey(ViewSource, on_delete=models.CASCADE)
    type = models.CharField(max_length=255)
    no_rows=models.IntegerField()
    is_consistent=models.IntegerField()
    is_complete=models.IntegerField()
    class Meta:
        managed = False
        db_table = 'etl_transaction'
        ordering = ['-id']

    
