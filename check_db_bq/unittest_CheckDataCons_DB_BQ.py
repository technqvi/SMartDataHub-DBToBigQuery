import unittest

from CheckDataCons_DB_BG import Get_ID_DB, Get_ID_BQ, find_diff_id, get_comming_data


class TestCheckDataCons_DB_BG(unittest.TestCase):

    def test_Get_ID_DB(self):
        df = Get_ID_DB()
        self.assertTrue(df.empty == False)

    def test_Get_ID_BQ(self):
        df = Get_ID_BQ()
        self.assertTrue(df.empty == False)

    def test_find_diff_id(self):
        dfPostgres = pd.DataFrame({'id': [1, 2, 3, 4, 5]})
        dfBigQuery = pd.DataFrame({'id': [1, 2, 3, 4, 6]})
        diffDB, diffBQ = find_diff_id(dfPostgres, dfBigQuery)
        self.assertTrue(diffDB == [5])
        self.assertTrue(diffBQ == [6])

    def test_get_comming_data(self):
        dbIDs = [1, 2, 3]
        result = get_comming_data(dbIDs, 'id')
        self.assertTrue(result == True)


if __name__ == '__main__':
    unittest.main()
