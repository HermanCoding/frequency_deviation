import date_range_data_extractor
import merge_clean_data

# data_extractor = date_range_data_extractor.DateRangeDataExtractor()
# data_extractor.extract_data(r'.\files', '2021-12-12', '2021-12-12')

merge_clean_data.merge_data(files_dir = r'./processed_files/')
