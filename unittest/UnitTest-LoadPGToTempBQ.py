import unittest
import LoadPGToTempBQ as x

class TestLoadPGToTempBQ(unittest.TestCase):

    def test_get_contentID_keyName(self):
        self.assertEqual(x.get_contentID_keyName("pmr_pm_plan"), (36, "pm_id", "merge_pm_plan"))
        self.assertEqual(get_contentID_keyName("pmr_pm_item"), (37, "pm_item_id", "merge_pm_item"))
        self.assertEqual(get_contentID_keyName("pmr_project"), (7, "project_id", "merge_project"))
        self.assertEqual(get_contentID_keyName("pmr_inventory"), (14, "inventory_id", "merge_inventory"))
        self.assertEqual(get_contentID_keyName("invalid_view_name"), None)

    def test_list_data(self):
        with self.subTest("with params"):
            sql = "SELECT * FROM table WHERE id = @id"
            params = {"id": 1}
            connection = psycopg2.connect(
                database="database", user="user", password="admin@smartapp4321.!", host="localh"
            )
            df = list_data(sql, params, connection)
            self.assertTrue(df.empty)

        with self.subTest("without params"):
            sql = "SELECT * FROM table"
            connection = psycopg2.connect(
                database="database", user="user", password="password", host="host"
            )
            df = list_data(sql, None, connection)
            self.assertTrue(df.empty)

    def test_get_bq_table(self):
        with self.subTest("table exists"):
            client = bigquery.Client()
            table_id = "project.dataset.table"
            table = client.get_table(table_id)
            self.assertEqual(table.table_type, "TABLE")

        with self.subTest("table does not exist"):
            client = bigquery.Client()
            table_id = "project.dataset.table"
            with self.assertRaises(NotFound):
                client.get_table(table_id)

    def test_insertDataFrameToBQ(self):
        with self.subTest("with empty dataframe"):
            df = pd.DataFrame()
            client = bigquery.Client()
            table_id = "project.dataset.table"
            with self.assertRaises(BadRequest):
                insertDataFrameToBQ(df, table_id)

        with self.subTest("with non-empty dataframe"):
            df = pd.DataFrame({"id": [1, 2, 3], "name": ["John", "Mary", "Bob"]})
            client = bigquery.Client()
            table_id = "project.dataset.table"
            insertDataFrameToBQ(df, table_id)
            rows_iter = client.list_rows(table_id, max_results=3)
            self.assertEqual(len(list(rows_iter)), 3)

    def test_checkFirstLoad(self):
        with self.subTest("main table is empty"):
            client = bigquery.Client()
            main_table_id = "project.dataset.table"
            rows_iter = client.list_rows(main_table_id, max_results=1)
            self.assertEqual(len(list(rows_iter)), 0)
            self.assertTrue(checkFirstLoad())

        with self.subTest("main table is not empty"):
            client = bigquery.Client()
            main_table_id = "project.dataset.table"
            rows_iter = client.list_rows(main_table_id, max_results=1)
            self.assertEqual(len(list(rows_iter)), 1)
            self.assertFalse(checkFirstLoad())

    def test_list_model_log(self):
        with self.subTest("with empty result"):
            sql = "SELECT * FROM log WHERE date_created >= @last_imported AND content_type_id = @content_id"
            params = {"last_imported": "2022-01-01 00:00:00", "content_id": 1}
            connection = psycopg2.connect(
                database="database", user="user", password="password", host="host"
            )
            df = list_model_log(sql, params, connection)
            self.assertTrue(df.empty)

        with self.subTest("with non-empty result"):
            sql = "SELECT * FROM log WHERE date_created >= @last_imported AND content_type_id = @content_id"
            params = {"last_imported": "2022-01-01 00:00:00", "content_id": 1}
            connection = psycopg2.connect(
                database="database", user="user", password="password", host="host"
            )
            df = list_model_log(sql, params, connection)
            self.assertFalse(df.empty)

    def test_check_any_changes_to_collumns_view(self):
        with self.subTest("with empty dataframe"):
            df = pd.DataFrame()
            check_any_changes_to_collumns_view(df, 1, 1)

        with self.subTest("with non-empty dataframe"):
            df = pd.DataFrame({"id": [1, 2, 3], "name": ["John", "Mary", "Bob"]})
            check_any_changes_to_collumns_view(df, 1, 1)

    def test_select_actual_action(self):
        with self.subTest("with empty dataframe"):
            df = pd.DataFrame()
            dfUpdateData = select_actual_action(df)
            self.assertTrue(dfUpdateData.empty)

        with self.subTest("with non-empty dataframe"):
            df = pd.DataFrame({"id": [1, 2, 3], "action": ["added", "changed", "deleted"]})
            dfUpdateData = select_actual_action(df)
            self.assertFalse(dfUpdateData.empty)

    def test_retrive_next_data_from_view(self):
        with self.subTest("with empty result"):
            sql = "select *  from view where id in (1, 2, 3)"
            df = retrive_next_data_from_view(sql, "id", [1, 2, 3])
            self.assertTrue(df.empty)

        with self.subTest("with non-empty result"):
            sql = "select *  from view where id in (1, 2, 3)"
            df = retrive_next_data_from_view(sql, "id", [1, 2, 3])
            self.assertFalse(df.empty)

    def test_retrive_first_data_from_view(self):
        with self.subTest("with empty result"):
            sql = "select *  from view where updated_at AT time zone 'utc' >= '2022-01-01 00:00:00'"
            df = retrive_first_data_from_view(sql, "2022-01-01 00:00:00")
            self.assertTrue(df.empty)

        with self.subTest("with non-empty result"):
            sql = "select *  from view where updated_at AT time zone 'utc' >= '2022-01-01 00:00:00'"
            df = retrive_first_data_from_view(sql, "2022-01-01 00:00:00")
            self.assertFalse(df.empty)

    def test_retrive_one_row_from_view_to_gen_df_schema(self):
        with self.subTest("with empty result"):
            sql = "select *  from view limit 1"
            df = retrive_one_row_from_view_to_gen_df_schema(sql)
            self.assertTrue(df.empty)

        with self.subTest("with non-empty result"):
            sql = "select *  from view limit 1"
            df = retrive_one_row_from_view_to_gen_df_schema(sql)
            self.assertFalse(df.empty)

    def test_add_acutal_action_to_df_at_next(self):
        with self.subTest("with empty dataframe"):
            df = pd.DataFrame()
            dfUpdateData = pd.DataFrame({"id": [1, 2, 3], "action": ["added", "changed", "deleted"]})
            x_view = "view"
            x_id = "id"
            df = add_acutal_action_to_df_at_next(df, dfUpdateData, x_view, x_id)
            self.assertTrue(df.empty)

        with self.subTest("with non-empty dataframe"):
            df = pd.DataFrame({"id": [1, 2, 3], "action": ["added", "changed", "deleted"]})
            dfUpdateData = pd.DataFrame({"id": [1, 2, 3], "action": ["added", "changed", "deleted"]})
            x_view = "view"
            x_id = "id"
            df = add_acutal_action_to_df_at_next(df, dfUpdateData, x_view, x_id)
            self.assertFalse(df.empty)

    def test_hasDplicateIDs(self):
        with self.subTest("with no duplicate ids"):
            df = pd.DataFrame({"id": [1, 2, 3]})
            hasDplicateIDs = df[df.duplicated()].any()
            self.assertFalse(hasDplicateIDs)

        with self.subTest("with duplicate ids"):
            df = pd.DataFrame({"id": [1, 1, 3]})
            hasDplicateIDs = df[df.duplicated()].any()
            self.assertTrue(hasDplicateIDs)

    def test_insertDataFrameToBQ(self):
        with self.subTest("with empty dataframe"):
            df = pd.DataFrame()
            client = bigquery.Client()
            table_id = "project.dataset.table"
            with self.assertRaises(BadRequest):
                insertDataFrameToBQ(df, table_id)

        with self.subTest("with non-empty dataframe"):
            df = pd.DataFrame({"id": [1, 2, 3], "name": ["John", "Mary", "Bob"]})
            client = bigquery.Client()
            table_id = "project.dataset.table"
            insertDataFrameToBQ(df, table_id)
            rows_iter = client.list_rows(table_id, max_results=3)
            self.assertEqual(len(list(rows_iter)), 3)

    def test_run_StoreProcedure_To_Merge_Temp_Main_and_Truncate_Transaction(self):
        with self.subTest("with empty dataframe"):
            df = pd.DataFrame()
            client = bigquery.Client()
            table_id = "project.dataset.table"
            insertDataFrameToBQ(df, table_id)
            rows_iter = client.list_rows(table_id, max_results=3)
            self.assertEqual(len(list(rows_iter)), 0)

        with self.subTest("with non-empty dataframe"):
            df = pd.DataFrame({"id": [1, 2, 3], "name": ["John", "Mary", "Bob"]})
            client = bigquery.Client()
            table_id = "project.dataset.table"
            insertDataFrameToBQ(df, table_id)
            rows_iter = client.list_rows(table_id, max_results=3)
            self.assertEqual(len(list(rows_iter)), 3)


if __name__ == "__main__":
    unittest.main()
