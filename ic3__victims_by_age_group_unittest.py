import unittest
import pandas as pd
import os

class TestData(unittest.TestCase):

    def setUp(self):
        # Create a sample DataFrame for testing
        parquet_directory_path  = "/home/TRYSAK/venv/files/ic3__victims_by_age_group/year=2016/state=Florida/090bb24cc23a4a6f91a83eaf96843cc3.parquet" 
        self.df = pd.read_parquet(parquet_directory_path)

    def test_file_not_empty(self):
        self.assertFalse(self.df.empty, "DataFrame should not be empty")

    def test_file_columns(self):
        expected_columns = ['age_range', 'count', 'amount_loss']
        self.assertListEqual(list(self.df.columns), expected_columns, "Columns do not match")

    def test_columns_data_types(self):
        self.assertTrue(self.df.dtypes.apply(lambda x: x.name).to_dict() == {"age_range":"string","count":"int32","amount_loss":"string"}, "Columns types not mached")

if __name__ == '__main__':
    unittest.main()
