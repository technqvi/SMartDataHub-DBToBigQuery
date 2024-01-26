
import unittest

from db_to_bq_by_bq_storage_api import db_to_bq_by_bq_storage_api


class TestDbToBqByBQStorageApi(unittest.TestCase):

    def test_db_to_bq_by_bq_storage_api(self):
        # Arrange
        csv_file = "incident_bq-storage-api.csv"
        view_name = "xyz_incident"
        view_name_id = "incident_id"
        datetimeCols = ["open_datetime", "close_datetime"]
        pk_fkCols = ['inventory_id', 'incident_id']
        projectId = "pongthorn"
        main_dataset_id = "SMartDataAnalytics"
        table_name = "incident"
        dt_imported = datetime.strptime(
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

        # Act
        result = db_to_bq_by_bq_storage_api(
            csv_file=csv_file,
            view_name=view_name,
            view_name_id=view_name_id,
            datetimeCols=datetimeCols,
            pk_fkCols=pk_fkCols,
            projectId=projectId,
            main_dataset_id=main_dataset_id,
            table_name=table_name,
            dt_imported=dt_imported,
        )

        # Assert
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
