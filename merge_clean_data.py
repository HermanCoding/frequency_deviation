import os
import pandas as pd

data = []


def merge_data(files_dir):
    for dirpath, dirnames, filenames in os.walk(files_dir):
        for filename in filenames:
            if filename.endswith('.csv'):
                print('Loading file {0}...'.format(filename))
                data.append(pd.read_csv(os.path.join(dirpath, filename)))
