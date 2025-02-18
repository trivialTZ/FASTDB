from django.db import migrations, models
from psqlextra.backend.migrations.operations import PostgresAddListPartition

class Migration(migrations.Migration):

    dependencies = [
        ('alerts', '0001_initial'),
    ]

    operations = [
        PostgresAddListPartition(
           model_name="DStoPVtoSS",
           name="processing_version_v1_2",
           values=["v1_2",],
        ),
         PostgresAddListPartition(
           model_name="DFStoPVtoSS",
           name="processing_version_v1_2",
           values=["v1_2",],
        ),
     PostgresAddListPartition(
           model_name="DiaSource",
           name="processing_version_v1_2",
           values=["v1_2",],
        ),
      PostgresAddListPartition(
           model_name="DiaForcedSource",
           name="processing_version_v1_2",
           values=["v1_2",],
        ),
      ]
