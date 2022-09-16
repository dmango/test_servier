import unittest
from etl import extract
import pandas as pd

class ExtractTest(unittest.TestCase):

    def test_extract_from_local(self):
        d = {'id': [1, 2], 'title': ["efferalgan", "chloroquine"], 'date':["01/01/2019", "01/02/2020"], 'journal': ["Le parisien", "gazetta"]}
        expected_df = pd.DataFrame(d)
        json_file = "test_write_json.json"
        csv_file = "test_write_csv.csv"
        with open(json_file, 'w') as f:
            expected_df.to_json(f, orient="records")
        with open(csv_file, 'w') as f:
            expected_df.to_csv(f, index=False)
        df = extract.extract_from_local(csv_file)
        self.assertEqual(expected_df.shape, df.shape)
        df = extract.extract_from_local(csv_file)
        self.assertEqual(expected_df.shape, df.shape)
        self.assertRaises(Exception, extract.extract_from_local("random"))

if __name__ == "__main__":
    unittest.main()
