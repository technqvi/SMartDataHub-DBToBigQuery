# unittest: test file
# ------Libery------------------------------
import unittest
from configupdater import ConfigUpdater
import pandas as pd

# ------Module------------------------------
from TestMain_CheckDataCons_DB_BQ import *


# ------Testing------------------------------
class TestCDC_DB_BQ(unittest.TestCase):

    # TEST CASE -------------------------------------------------------------------------
    # TEST 1
    def test_get_id_db(self): #To test can get id from db
        dfDB = Get_ID_DB()
        self.assertFalse(dfDB.empty)
    
    
    # TEST 2
    def test_get_id_bq(self): #To test can get id from bq
        dfBQ = Get_ID_BQ()
        self.assertFalse(dfBQ.empty)


    # TEST 3
    def test_different_length_lists(self): #To test can find different ID
        dfPostgres = pd.DataFrame({'project_id': [1, 2, 3, 4]})
        dfBigQuery = pd.DataFrame({'project_id': [1, 2, 3]})
        diffDB, diffBQ = find_diff_id(dfPostgres, dfBigQuery)
        self.assertEqual(diffDB, [4])
        self.assertEqual(diffBQ, [])
    
    
    # TEST 4
    def test_export_inconsistent_data_with_empty_list(self): #To test can export inconsistent
        x_dbIDs = []
        id = 'id'
        result = export_inconsistent_data(x_dbIDs, id)
        self.assertFalse(result)



if __name__ == '__main__':
    unittest.main()
