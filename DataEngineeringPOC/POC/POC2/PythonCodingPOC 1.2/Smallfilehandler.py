import os
from datetime import datetime, timedelta

class SmallFileHandler:
    def __init__(self, base_path):
        self.base_path = base_path

    def read_data_for_day(self, date):
        """Reads data for a specific day."""
        date_str = date.strftime('%Y-%m-%d')
        file_path = os.path.join(self.base_path, f"ses_dts={date_str}.parquet")

        if os.path.exists(file_path):
            print(f"Reading data from {file_path}")
            return True
        else:
            print(f"No data found for {date_str}")
            return False

    def execute_process_data(self, start_date, end_date):
        """Processes data between two dates."""
        current_date = start_date
        while current_date <= end_date:
            success = self.read_data_for_day(current_date)
            if not success:
                print(f"Failed to process data for {current_date}")
            current_date += timedelta(days=1)

    def run_for_period(self, period_type, start_date=None, end_date=None):
        """Runs processing based on period type."""
        if period_type == 'day':
            if start_date is None:
                raise ValueError("Start date must be provided for daily processing.")
            self.read_data_for_day(start_date)

        elif period_type == 'week':
            if start_date is None or end_date is None:
                raise ValueError("Both start and end dates must be provided for weekly processing.")
            self.execute_process_data(start_date, end_date)

        elif period_type == 'month':
            # Assuming month means processing all days in that month
            if start_date is None:
                raise ValueError("Start date must be provided for monthly processing.")

            first_day_of_month = start_date.replace(day=1)
            next_month = first_day_of_month.month % 12 + 1
            year_increment = first_day_of_month.year + (first_day_of_month.month // 12)

            last_day_of_month = (first_day_of_month.replace(year=year_increment, month=next_month) - timedelta(days=1))

            self.execute_process_data(first_day_of_month, last_day_of_month)

        elif period_type == 'year':
            # Assuming year means processing all days in that year
            if start_date is None:
                raise ValueError("Start date must be provided for yearly processing.")

            last_day_of_year = start_date.replace(month=12, day=31)

            self.execute_process_data(start_date, last_day_of_year)

