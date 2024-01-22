import unittest

import psycopg2
import pandas as pd
import m01_CheckDataCons_DB_BQ as m01

# https://adamj.eu/tech/2020/10/13/how-to-mock-environment-variables-with-pythons-unittest/
# https://machinelearningmastery.com/a-gentle-introduction-to-unit-testing-in-python/
# https://docs.python.org/3/library/unittest.html#unittest.TestCase.setUp
class TestCheckDataCons_DB_BQ(unittest.TestCase):
    
    def test_get_postgres_conn(self):
        # Arrange
        expected_conn = psycopg2.connect(
            database='test_database', user='test_user',
            password='test_password', host='test_host'
        )

        # Act
        actual_conn = m01.get_postgres_conn()

        # Assert
        self.assertEqual(expected_conn, actual_conn)

    def test_list_data_pg(self):
        # Arrange
        sql = 'SELECT * FROM test_table'
        params = None
        connection = m01.get_postgres_conn()

        # Act
        df = m01.list_data_pg(sql, params, connection)

        # Assert
        self.assertTrue(df.empty)

    def test_load_data_bq(self):
        # Arrange
        sql = 'SELECT * FROM test_table'

        # Act
        df = m01.load_data_bq(sql)

        # Assert
        self.assertTrue(df.empty)

    def test_find_diff_id(self):
        # Arrange
        dfPostgres = pd.DataFrame({'id': [1, 2, 3]})
        dfBigQuery = pd.DataFrame({'id': [1, 2, 4]})

        # Act
        diffDB, diffBQ = m01.find_diff_id(dfPostgres, dfBigQuery)

        # Assert
        self.assertEqual(diffDB, [3])
        self.assertEqual(diffBQ, [4])

    def test_export_inconsistent_data(self):
        # Arrange
        dbIDs = [1, 2]
        id = 'id'

        # Act
        result = m01.export_inconsistent_data(dbIDs, id)

        # Assert
        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()
