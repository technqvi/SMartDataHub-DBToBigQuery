# unittest: test file
# ------Libery------------------------------
import unittest
from google.cloud import bigquery
import pandas as pd
from unittest.mock import patch
from unittest.mock import MagicMock, patch
from unittest.mock import Mock
from unittest.mock import patch, MagicMock


# ------Module------------------------------
from TestMain_LoadPGToBQ import * 
# from LoadPGToBQ import * # can't use to import
# from TestMain_LoadPGToBQ import * # can't use to import


# ------Testing------------------------------
class TestPMR(unittest.TestCase):

#SETUP TEST-------------------------------------------------------------------------
    def setUp(self):
        # Set up Parameters
        self.view_name = "pmr_pm_plan"
        self.view_name_id = "pm_id"
        self.last_imported = "2019-01-01 00:00:00"
        self.key_name = "key_name"
        
        
        # Set up BigQuery client
        credentials = service_account.Credentials.from_service_account_file(config['PROJECT_CREDENTIAL_FILE'])
        self.client = bigquery.Client(credentials=credentials, project=config['PROJECT_ID'])


        # Set up table for testing
        self.table_name = "etl_transaction"
        self.table_id = f"{projectId}.{dataset_id}.{table_name}"
        self.check_consistency=False
        self.changed_field_mapping = {"name", "age", "tell"}
        
        
#TEST CASE -------------------------------------------------------------------------
#TEST 1
    def test_get_config_file(self): #To test it's can get config
        config, updater, data_base_file = get_config_file()
        self.assertTrue(config)
        self.assertTrue(updater)
        self.assertTrue(data_base_file)


#TEST 2
    def test_list_data_sqlite(self): #To test it's can list data
        sql = f"SELECT * FROM {self.table_name}"
        df_item = list_data_sqlite(sql)
        print("****************************************************************************************")
        print(df_item)
        print("****************************************************************************************")
        self.assertFalse(df_item.empty, "DataFrame should not be empty")


#TEST 3
    def test_get_view_source(self): #To test it's can get view source
        view_source = get_view_source(self.view_name)
        print("****************************************************************************************")
        print(view_source)
        print("****************************************************************************************")
        self.assertFalse(view_source.empty, "View source should not be empty")


#TEST 4
    def test_get_postgres_conn(self): #To test it's can connect to postgres
        conn = get_postgres_conn()
        print("****************************************************************************************")
        print(conn)
        print("****************************************************************************************")
        self.assertTrue(conn)


#TEST 5        
    def test_check_fileds_in_view_source_in_web_admin_existing_in_database_with_missing_column(self): #To test it's have fields
        col_list = ["pm_id","project_id", "planned_date", "ended_pm_date","pm_period","team_lead", "updated_at"]
        x_check = check_fileds_in_view_source_in_web_admin_existing_in_database(self.view_name, col_list)
        print("****************************************************************************************")
        print(x_check)
        print("****************************************************************************************")
        self.assertTrue(x_check)


#TEST 6        
    def test_check_existing_table_return_schema(self): #To test it's can check schema
        table = bigquery.Table(self.table_id)
        table.schema = [
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("age", "INTEGER"),
        ]
        try:
            self.client.create_table(table)
        except:
            pass
        result = check_existing_table_return_schema(self.client, self.table_id)
        print("****************************************************************************************")
        print(result)
        print("****************************************************************************************")
        self.assertIsNotNone(result)
        

#TEST 7        
    def test_get_bq_table(self): #To test it's can connect to bq server
        client = bigquery.Client()
        table = client.get_table(self.table_id)
        print("****************************************************************************************")
        print(table)
        print("****************************************************************************************")
        self.assertTrue(table)
        
    @patch('TestMain_LoadPGToBQ.client')  
    def test_load_data_bq(self, mock_client):
        test_sql = f"SELECT * FROM {self.table_id}"
        mock_query_result = MagicMock()
        mock_client.query.return_value = mock_query_result
        mock_query_result.to_dataframe.return_value = MagicMock()
        result_df = load_data_bq(test_sql)
        mock_client.query.assert_called_once_with(test_sql)
        mock_query_result.to_dataframe.assert_called_once()
        print("****************************************************************************************")
        print(result_df)
        print("****************************************************************************************")
        print(mock_query_result)
        print("****************************************************************************************")
        self.assertEqual(result_df, mock_query_result.to_dataframe.return_value)
     
        
#TEST 8
    def test_do_check_consistency_with_check_consistency_true(self): #To test consistency is working
        self.check_consistency = True
        result = do_check_consistency()
        print("****************************************************************************************")
        print(result)
        print("****************************************************************************************")
        self.assertTrue(result)


#TEST 9
    def test_add_tran_with_x_no_rows_and_x_is_complete(self): #To test transaction
        x_no_rows = 10
        x_is_complete = True
        add_tran(x_no_rows, x_is_complete)
        self.assertTrue(True)


#TEST 10    
    @patch('TestMain_LoadPGToBQ.list_data')  # Mock the list_data function
    def test_list_model_log(self, mock_list_data): #To test can list data
        x_content_id = 1
        expected_output = [
            {
                "object_id": 1,
                "action": "added",
                "date_created": "2019-01-01 00:00:00",
                "changed_data": "{}"
            },
            {
                "object_id": 2,
                "action": "deleted",
                "date_created": "2019-01-01 00:02:00",
                "changed_data": "{}"
            }
        ]
        mock_list_data.return_value = pd.DataFrame(expected_output)
        actual_output = list_model_log(self.last_imported, x_content_id)
        pd.testing.assert_frame_equal(pd.DataFrame(expected_output), actual_output)


#TEST 11
    @patch('TestMain_LoadPGToBQ.changed_field_mapping', {"name", "age", "day"})
    def test_find_change_in_list_mapping(self): # To test can find change in changed_field
        changed_data = {
            "name": "testing",
            "x": "101",
            "y": "0123456789",
        }
        result = findChangeInListMapping(changed_data)
        self.assertTrue(result, f"Expected True, but got False. Changed data: {changed_data}, Changed fields: {changed_field_mapping}")        


#TEST 12
    def test_check_no_changes_to_columns_view_only_changed_action(self): # To test can check no change in changed action
        dfAction = pd.DataFrame({
            "object_id": [1, 1, 1],
            "action": ["added", "changed", "changed"],
            "changed_data": [{"name": "testing"}, {"age": "101"}, {"tell": "0123456789"}]
        })
        result = check_no_changes_to_columns_view_only_changed_action(dfAction, self.view_name, self.key_name)
        self.assertFalse(result)


#TEST 13    
    def test_select_actual_action(self): # To test can select actual action
        sample_data = {
            'object_id': [1, 1, 2, 3, 3, 3],
            'action': ['added', 'changed', 'added', 'added', 'changed', 'deleted']
        }
        lf = pd.DataFrame(sample_data)
        result = select_actual_action(lf)
        expected_result = pd.DataFrame({
            'id': [1, 2],
            'action': ['added', 'added']
        })
        pd.testing.assert_frame_equal(result, expected_result)
        
        
#TEST 14    
    def test_process_actual_acton_selecting_with_import(self): # To test it's working
        isFirstLoad = False
        last_imported = "2022-09-01 00:00:00"
        content_id = 1
        add_tran = Mock()
        list_model_log = Mock()
        sample_data = {
            'id': [1, 2, 3],
            'action': ['added', 'updated', 'deleted']
        }
        list_model_log.return_value = pd.DataFrame(sample_data)
        listForRemove = [2]
        select_actual_action = Mock()
        select_actual_action.return_value = pd.DataFrame({
            'id': [1, 3],
            'action': ['added', 'deleted']
        })
        listModelLogObjectIDs, dfModelLog = process_actual_acton_selecting(
            isFirstLoad, last_imported, content_id, add_tran, list_model_log, listForRemove, select_actual_action
        )
        add_tran.assert_not_called()
        self.assertEqual(listModelLogObjectIDs, [1, 3])
        self.assertEqual(dfModelLog.shape, (2, 2))
        
        
#TEST 15 
    def test_retrive_next_data_from_view_with_non_empty_listmodelLogObjectIDs(self): # To test retrive next data
        x_listModelLogObjectIDs = [17, 22, 31]
        df = retrive_next_data_from_view(self.view_name, self.view_name_id, x_listModelLogObjectIDs)
        print("****************************************************************************************")
        print(df)
        print("****************************************************************************************")
        self.assertFalse(df.empty)


#TEST 16 
    def test_retrive_first_data_from_view_with_non_empty_result(self): # To test retrive first data
        x_last_imported = '2022-09-01 00:00:00'
        df = retrive_first_data_from_view(self.view_name, x_last_imported)
        print("****************************************************************************************")
        print(df)
        print("****************************************************************************************")
        self.assertFalse(df.empty)
        
        
#TEST 17        
    def test_retrive_one_row_from_view_to_gen_df_schema_for_all_deleted_action_with_non_empty_result(self): # To test retrive one row from view
        df = retrive_one_row_from_view_to_gen_df_schema_for_all_deleted_action(self.view_name)
        print("****************************************************************************************")
        print(df)
        print("****************************************************************************************")
        self.assertFalse(df.empty)


#TEST 18
    def test_process_data_retrival_from_view_with_one_row_in_model_log(self): # To test function process data retrival
        isFirstLoad = True
        view_name_id = 1
        add_tran = lambda x, y: None
        listModelLogObjectIDs = [1]
        retrive_next_data_from_view = lambda x, y, z: pd.DataFrame({"column_name": ["value"]})
        retrive_first_data_from_view = lambda x, y: pd.DataFrame({"column_name": ["value"]})
        retrive_one_row_from_view_to_gen_df_schema_for_all_deleted_action = lambda x: pd.DataFrame({"column_name": ["value"]})

        df = process_data_retrival_from_view(self.view_name, isFirstLoad, self.last_imported, view_name_id, add_tran, listModelLogObjectIDs, retrive_next_data_from_view, retrive_first_data_from_view, retrive_one_row_from_view_to_gen_df_schema_for_all_deleted_action)
        print("****************************************************************************************")
        print(df)
        print("****************************************************************************************")
        self.assertEqual(df.shape, (1, 1))  


#TEST 19
    def test_add_acutal_action_to_df_at_next(self): # To test function add acutal action to df at next *not sure*
        df = pd.DataFrame({
            'pm_id': [17, 20, 31],
            'action': ['added', 'changed', 'deleted']
        })
        dfUpdateData = pd.DataFrame({
            'id': [17, 20],
            'action': ['deleted', 'changed']
        })
        merged_df = add_acutal_action_to_df_at_next(df, dfUpdateData, self.view_name, self.view_name_id)

        # Print merged DataFrame for debugging
        print("Merged DataFrame:")
        print(merged_df)

        # Assert the expected behavior
        self.assertTrue('action_x' in merged_df.columns or 'action_y' in merged_df.columns, f"'action' column not found in columns: {merged_df.columns}")  # Check if 'action' column is in the merged DataFrame
        self.assertTrue('deleted' in merged_df['action_x'].values or 'deleted' in merged_df['action_y'].values, "'deleted' action not found in 'action' column")


    
        
#TEST 20        
    def test_transform_data_with_no_duplicate_ids(self): # To test no duplicate
        id = 'id'
        pk_fk_list = []
        df = pd.DataFrame({'id': [1, 2, 3]})
        df = transform_data(id, pk_fk_list, df)
        print("****************************************************************************************")
        print(df)
        print("****************************************************************************************")
        self.assertFalse(df[id].duplicated().any())
        
    
    
if __name__ == '__main__':
    unittest.main()