import unittest
from datetime import datetime, timedelta
from Smallfilehandler import SmallFileHandler
import os
import tempfile
import shutil

class TestSmallFileHandler(unittest.TestCase):

    def setUp(self):
        self.base_path = tempfile.mkdtemp()
        self.small_file_handler = SmallFileHandler(self.base_path)

    def test_read_data_for_day(self):
        date = datetime(2022, 1, 1)
        file_path = os.path.join(self.base_path, f"ses_dts={date.strftime('%Y-%m-%d')}.parquet")
        with open(file_path, 'w') as f:
            f.write("test data")
        self.assertTrue(self.small_file_handler.read_data_for_day(date))

    def test_read_data_for_day_no_file(self):
        date = datetime(2022, 1, 1)
        self.assertFalse(self.small_file_handler.read_data_for_day(date))

    def test_execute_process_data(self):
        start_date = datetime(2022, 1, 1)
        end_date = datetime(2022, 1, 3)
        for i in range((end_date - start_date).days + 1):
            date = start_date + timedelta(days=i)
            file_path = os.path.join(self.base_path, f"ses_dts={date.strftime('%Y-%m-%d')}.parquet")
            with open(file_path, 'w') as f:
                f.write("test data")
        self.small_file_handler.execute_process_data(start_date, end_date)

    def test_run_for_period_day(self):
        date = datetime(2022, 1, 1)
        file_path = os.path.join(self.base_path, f"ses_dts={date.strftime('%Y-%m-%d')}.parquet")
        with open(file_path, 'w') as f:
            f.write("test data")
        self.small_file_handler.run_for_period('day', date)

    def test_run_for_period_week(self):
        start_date = datetime(2022, 1, 1)
        end_date = datetime(2022, 1, 7)
        for i in range((end_date - start_date).days + 1):
            date = start_date + timedelta(days=i)
            file_path = os.path.join(self.base_path, f"ses_dts={date.strftime('%Y-%m-%d')}.parquet")
            with open(file_path, 'w') as f:
                f.write("test data")
        self.small_file_handler.run_for_period('week', start_date, end_date)

    def test_run_for_period_month(self):
        start_date = datetime(2022, 1, 1)
        last_day_of_month = datetime(2022, 1, 31)
        for i in range((last_day_of_month - start_date).days + 1):
            date = start_date + timedelta(days=i)
            file_path = os.path.join(self.base_path, f"ses_dts={date.strftime('%Y-%m-%d')}.parquet")
            with open(file_path, 'w') as f:
                f.write("test data")
        self.small_file_handler.run_for_period('month', start_date)

    def test_run_for_period_year(self):
        start_date = datetime(2022, 1, 1)
        last_day_of_year = datetime(2022, 12, 31)
        for i in range((last_day_of_year - start_date).days + 1):
            date = start_date + timedelta(days=i)
            file_path = os.path.join(self.base_path, f"ses_dts={date.strftime('%Y-%m-%d')}.parquet")
            with open(file_path, 'w') as f:
                f.write("test data")
        self.small_file_handler.run_for_period('year', start_date)

if __name__ == '__main__':
    unittest.main()