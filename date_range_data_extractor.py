import os
import pandas as pd


class DateRangeDataExtractor:
    def __init__(self):
        self.data = []

    def extract_data(self, files_dir, start_date_str, end_date_str):
        start_date = pd.to_datetime(start_date_str, format='%Y-%m-%d')
        end_date = pd.to_datetime(end_date_str, format='%Y-%m-%d')

        for dirpath, dirnames, filenames in os.walk(files_dir):
            for filename in filenames:
                if filename.endswith('.csv'):
                    date = self.get_date_from_filename(filename)
                    if date is not None and start_date <= date <= end_date:
                        print('Loading file {0}...'.format(filename))
                        self.data.append(pd.read_csv(os.path.join(dirpath, filename)))

    @staticmethod
    def get_date_from_filename(filename):
        date_part = filename.split('.csv')[0]
        try:
            date = pd.to_datetime(date_part, format='%Y-%m-%d')
            return date
        except ValueError:
            print(f'Warning: Skipping file {filename} - Invalid date format.')
            return None
