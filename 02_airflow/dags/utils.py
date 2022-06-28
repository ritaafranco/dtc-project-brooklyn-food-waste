

class Configs():
    # class with variables used across multiple notebooks
    raw_folder = 'raw'
    processed_folder = 'processed'
    
    dataset_file = 'brooklyn.csv'
    dataset_name = 'ursulakaczmarek/brooklyn-food-waste'
    zipfile_name = dataset_name.split('/')[1]+'.zip'
    parquet_file = dataset_file.replace('.csv', '.parquet')
    input_filetype = 'PARQUET'
    bq_food_waste_table_name = 'food_waste'