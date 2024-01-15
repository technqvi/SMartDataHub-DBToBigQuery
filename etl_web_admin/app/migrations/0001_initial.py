# Generated by Django 4.2.9 on 2024-01-15 14:41

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='ETLTransaction',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('trans_datetime', models.CharField(max_length=100)),
                ('no_rows', models.IntegerField()),
                ('is_consistent', models.IntegerField()),
                ('is_complete', models.IntegerField()),
            ],
            options={
                'db_table': 'etl_transaction',
                'ordering': ['-id'],
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='ViewSource',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=255)),
                ('load_type', models.CharField(choices=[('merge', 'merge'), ('bq-storage-api', 'bq-storage-api')], default='0', max_length=50, verbose_name='CDC-Load To BQ Type')),
                ('app_conten_type_i', models.IntegerField()),
                ('app_key_name', models.CharField(max_length=255)),
                ('app_changed_field_mapping', models.TextField(help_text='filed1,filed2,filed3')),
            ],
            options={
                'db_table': 'view_source',
                'ordering': ['id'],
                'managed': False,
            },
        ),
    ]