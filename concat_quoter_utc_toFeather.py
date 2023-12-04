import os
import pandas as pd
import merge_clean_data


class ConcatQuoterUtcToFeather:
    def __init__(self, folder_name, file_name):
        self.folder_name = folder_name
        self.file_name = file_name

    def concat_quoter_utc(self):
        merge_clean_data.merge_data(files_dir=r'./processed_files/')
        data = merge_clean_data.data
        df = pd.concat(data, ignore_index=True, join='inner')
        print('Data concatenated')

        df['Time'] = pd.to_datetime(df['Time'], utc=True)
        print('Time data set to the datetime type with UTC')

        df['Time'] = pd.to_datetime(df['Time']).dt.tz_convert('Europe/Stockholm')
        print('UTC changed to Europe/Stockholm')

        df.set_index('Time', inplace=True)
        print('Time column set as Index')

        if not os.path.exists(self.folder_name):
            os.makedirs(self.folder_name)

        file_path = os.path.join(self.folder_name, self.file_name)
        df.to_feather(file_path)
        print('Binary feather file created of dataframe')
