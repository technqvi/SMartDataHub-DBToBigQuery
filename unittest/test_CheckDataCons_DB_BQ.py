import unittest

class TestCheckDataCons_DB_BQ(unittest.TestCase):

    def test_get_postgres_conn(self):
        conn = get_postgres_conn()
        self.assertTrue(conn is not None)

    def test_list_data_pg(self):
        sql = "SELECT * FROM etl_transaction"
        params = None
        connection = get_postgres_conn()
        df = list_data_pg(sql, params, connection)
        self.assertTrue(df is not None)
        self.assertTrue(df.empty is False)

    def test_load_data_bq(self):
        sql = "SELECT * FROM my_table"
        df = load_data_bq(sql)
        self.assertTrue(df is not None)
        self.assertTrue(df.empty is False)

    def test_Get_ID_DB(self):
        df = Get_ID_DB()
        self.assertTrue(df is not None)
        self.assertTrue(df.empty is False)

    def test_Get_ID_BQ(self):
        df = Get_ID_BQ()
        self.assertTrue(df is not None)
        self.assertTrue(df.empty is False)

    def test_find_diff_id(self):
        dfPostgres = pd.DataFrame([
            {
                "id": 1,
                "name": "John Doe"
            },
            {
                "id": 2,
                "name": "Jane Doe"
            }
        ])
        dfBigQuery = pd.DataFrame([
            {
                "id": 1,
                "name": "John Doe"
            },
            {
                "id": 3,
                "name": "Michael Jones"
            }
        ])
        diffDB, diffBQ = find_diff_id(dfPostgres, dfBigQuery)
        self.assertTrue(diffDB == [2])
        self.assertTrue(diffBQ == [3])

    def test_export_inconsistent_data(self):
        dbIDs = [1, 2]
        id = "id"
        result = export_inconsistent_data(dbIDs, id)
        self.assertTrue(result is True)

if __name__ == '__main__':
    unittest.main()
