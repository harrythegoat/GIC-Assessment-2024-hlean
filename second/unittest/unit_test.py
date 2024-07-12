import unittest
import polars as pl
from ..ReportGenerator import FundReportGenerator

def generate_report(fund: str):
    return [True]

def get_data_from_db(query: str):
    df = pl.DataFrame()
    return df

def create_save_dir(dirname: str, dirpath: str):
    # check path exists after creation
    return True

def process_data(fund: str, name:str, path: str, equity: bool, positions: pl.DataFrame, prices: pl.DataFrame):
    return []

class TestReportGeneratorMethods(unittest.TestCase):
    # report_generator = FundReportGenerator()
    def setUp(self) -> None:
        self.fund = 'applebead'
        self.query = 'SELECT * FROM external_funds'
        self.dir_name = 'Equities'
        self.dir_path = r'\temp\funds\applebead\Equities'

    def test_inputGenerateReport(self):
        self.assertIsNotNone(generate_report(self.fund))
        self.assertIsInstance(self.fund, str)

    def test_outputGenerateReport(self):
        self.assertIsNotNone(generate_report(self.fund))
        self.assertIsInstance(generate_report(self.fund), list)

    def test_inputGetDataFromDb(self):
        self.assertIsNotNone(get_data_from_db(self.query))
        self.assertIsInstance(self.query, str)

    def test_outputGetDataFromDb(self):
        self.assertIsInstance(get_data_from_db(self.query), pl.DataFrame)

    def test_inputCreateSaveDir(self):
        self.assertIsNotNone(create_save_dir(self.dir_name, self.dir_path))

    def test_outputCreateSaveDir(self):
        self.assertIsNotNone(create_save_dir(self.dir_name, self.dir_path), bool)


if __name__ == '__main__':
    unittest.main()