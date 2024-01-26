import unittest

class TestLoadPGToBQ(unittest.TestCase):

    def test_get_config_file(self):
        config, updater, data_base_file = get_config_file()
        self.assertTrue(config is not None)
        self.assertTrue(updater is not None)
        self.assertTrue(data_base_file is not None)

    def test_list_data_sqlite(self):
        sql = "SELECT * FROM etl_transaction"
        df_item = list_data_sqlite(sql)
        self.assertTrue(df_item is not None)
        self.assertTrue(df_item.empty is False)

    def test_addETLTrans(self):
        recordList = [
            (datetime.now(timezone.utc), 1, 100, True, True)
        ]
        addETLTrans(recordList)

    def test_get_postgres_conn(self):
        conn = get_postgres_conn()
        self.assertTrue(conn is not None)

    def test_list_data(self):
        sql = "SELECT * FROM pm_item"
        params = None
        connection = get_postgres_conn()
        df = list_data(sql, params, connection)
        self.assertTrue(df is not None)
        self.assertTrue(df.empty is False)

    def test_get_view_source(self):
        name = "pm_item"
        view_source = get_view_source(name)
        self.assertTrue(view_source is not None)

    def test_check_fileds_in_view_source_in_web_admin_existing_in_database(self):
        x_name = "pm_item"
        x_list = ["id", "item_code", "item_name"]
        result = check_fileds_in_view_source_in_web_admin_existing_in_database(x_name, x_list)
        self.assertTrue(result is True)

    def test_do_check_consistency(self):
        result = do_check_consistency()
        self.assertTrue(result is True)

    def test_add_tran(self):
        x_no_rows = 100
        x_is_complete = True
        add_tran(x_no_rows, x_is_complete)

    def test_checkFirstLoad(self):
        result = checkFirstLoad()
        self.assertTrue(result is True)

    def test_list_model_log(self):
        x_last_imported = "2022-09-08 00:00:00"
        x_content_id = 1
        df = list_model_log(x_last_imported, x_content_id)
        self.assertTrue(df is not None)
        self.assertTrue(df.empty is False)

    def test_findChangeInListMapping(self):
        changed_data = {
            "id": 1,
            "item_code": "ABC",
            "item_name": "XYZ"
        }
        result = findChangeInListMapping(changed_data)
        self.assertTrue(result is True)

    def test_check_no_changes_to_columns_view_only_changed_action(self):
        dfAction = pd.DataFrame([
            {
                "object_id": 1,
                "action": "added",
                "changed_data": "{}"
            },
            {
                "object_id": 2,
                "action": "deleted",
                "changed_data": "{}"
            }
        ])
        x_view_name = "pm_item"
        _x_key_name = "id"
        result = check_no_changes_to_columns_view_only_changed_action(dfAction, x_view_name, _x_key_name)
        self.assertTrue(result is True)

    def test_select_actual_action(self):
        dfModelLog = pd.DataFrame([
            {
                "object_id": 1,
                "action": "added",
                "date_created": "2022-09-08 00:00:00",
                "changed_data": "{}"
            },
            {
                "object_id": 2,
                "action": "deleted",
                "date_created": "2022-09-09 00:00:00",
                "changed_data": "{}"
            }
        ])
        dfUpdateData = select_actual_action(dfModelLog)
        self.assertTrue(dfUpdateData is not None)
        self.assertTrue(dfUpdateData.empty is False)

    def test_retrive_next_data_from_view(self):
        x_view = "pm_item"
        x_id = "id"
        x_listModelLogObjectIDs = [1, 2]
        df = retrive_next_data_from_view(x_view, x_id, x_listModelLogObjectIDs)
        self.assertTrue(df is not None)
        self.assertTrue(df.empty is False)

    def test_retrive_first_data_from_view(self):
        x_view = "pm_item"
        x_last_imported = "2022-09-08 00:00:00"
        df = retrive_first_data_from_view(x_view, x_last_imported)
        self.assertTrue(df is not None)
        self.assertTrue(df.empty is False)

    def test_retrive_one_row_from_view_to_gen_df_schema_for_all_deleted_action(self):
        x_view = "pm_item"
        df = retrive_one_row_from_view_to_gen_df_schema_for_all_deleted_action(x_view)
        self.assertTrue(df is not None)
        self.assertTrue(df.empty is False)

    def test_add_acutal_action_to_df_at_next(self):
        df = pd.DataFrame([
            {
                "id": 1,
                "item_code": "ABC",
                "item_name": "XYZ"
            },
            {
                "id": 2,
                "item_code": "DEF",
                "item_name": "GHI"
            }
        ])
        dfUpdateData = pd.DataFrame([
            {
                "id": 1,
                "action": "added"
            },
            {
                "id": 2,
                "action": "deleted"
            }
        ])
        x_view = "pm_item"
        x_id = "id"
        merged_df = add_acutal_action_to_df_at_next(df, dfUpdateData, x_view, x_id)
        self.assertTrue(merged_df is not None)
        self.assertTrue(merged_df.empty is False)

    def test_df_vs_bq(self):
        dFColsV = ["id", "item_code", "item_name"]
        bQColsV = ["id", "item_code", "item_name"]
        result = df_vs_bq(dFColsV, bQColsV)
        self.assertTrue(result is True)

    def test_insertDataFrameToBQ(self):
        df = pd.DataFrame([
            {
                "id": 1,
                "item_code": "ABC",
                "item_name": "XYZ"
            },
            {
                "id": 2,
                "item_code": "DEF",
                "item_name": "GHI"
            }
        ])
        insertDataFrameToBQ(df)

    def test_bq_cdc_stream_loader(self):
        csv_file = "test.csv"
        view_name = "pm_item"
        view_name_id = "id"
        datetimeCols = []
        pk_fkCols = []
        projectId = "my-project"
        main_dataset_id = "my-dataset"
        table_name = "my-table"
        dt_imported = datetime.now(timezone.utc)
        resutl = bq_cdc_stream_loader.db_to_bq_by_bq_storage_api(
            csv_file=csv_file,
            view_name=view_name,view_name_id=view_name_id,
            datetimeCols=datetimeCols,pk_fkCols=pk_fkCols,
            projectId=projectId,main_dataset_id= main_dataset_id,table_name=data_name,
            dt_imported=dt_imported    
        )   
        self.assertTrue(resutl is not None)

if __name__ == '__main__':
    unittest.main()
