import unittest
from pathlib import Path
import pandas as pd
from pandas.api.types import is_string_dtype
from etl import transform

class TransformTest(unittest.TestCase):

    def test_clean_drugs_df(self):
        d = {'atccode': ["A04AD", "S03AA"], 'drug': ["Efferalgan", "Chloroquine"]}
        expected_df = pd.DataFrame(d)
        parquet_file = "drugs.parquet"
        expected_df.to_parquet(parquet_file)
        df = transform.clean_drugs_df(parquet_file)
        self.assertTrue("id" in df.columns)
        self.assertTrue(df["drug"].str.islower().all())

    def test_clean_pubmed_df(self):
        d = {'id': [1, 2], 'title': ["efferalgan", "chloroquine"], 'date':["01/01/2019", "01/02/2020"], 'journal': ["Le parisien", "gazetta"]}
        expected_df = pd.DataFrame(d)
        parquet_file = "pubmed.parquet"
        expected_df.to_parquet(parquet_file)
        df = transform.clean_pubmed_df(parquet_file)
        self.assertTrue(is_string_dtype(df['date']))
    
    def test_clean_clinicaltrial_df(self):
        d = {'id': [1, 2], 'scientific_title': ["efferalgan", "chloroquine"], 'date':["01/01/2019", "01/02/2020"], 'journal': ["Le parisien", "gazetta"]}
        expected_df = pd.DataFrame(d)
        parquet_file = "clinicaltrial.parquet"
        expected_df.to_parquet(parquet_file)
        df = transform.clean_clinicaltrial_df(parquet_file)
        self.assertTrue(is_string_dtype(df['date']))
    
    def test_concat_pubmed(self):
        d = {'id': [1, 2], 'title': ["efferalgan", "chloroquine"], 'date':["01/01/2019", "01/02/2020"], 'journal': ["Le parisien", "gazetta"]}
        expected_df = pd.DataFrame(d)
        json_file = "pubmedjson"
        csv_file = "pubmedcsv"
        expected_df.to_parquet(json_file)
        expected_df.to_parquet(csv_file)
        df = transform.concat_pubmed(json_file, csv_file)
        self.assertEqual(df.shape[0], expected_df.shape[0]*2)
    
    def test_find_drug_in_title(self):
        row = "chloroquine can heal covid-19"
        searched_drugs = ["chloroquine", "efferalgan"]
        found_drugs = transform.find_drug_in_title(row, searched_drugs)
        self.assertEqual(found_drugs, ["chloroquine"])



if __name__ == "__main__":
    unittest.main()