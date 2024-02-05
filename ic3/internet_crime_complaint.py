import requests
import pandas as pd
import os
from bs4 import BeautifulSoup
import pyarrow as pa
import pyarrow.parquet as pq
import json
import logging

class InternetCrimeComplaintLoad:

    def __init__(self,
                 url, 
                 local_folder_path,
                 tables,
                 years,
                 df_dtype
                 ) -> None:
        self.url = url
        self.local_folder_path = local_folder_path
        self.tables = tables
        self.years = years
        self.df_dtype = df_dtype
        
    def init_logging(self) -> None:
        logging.basicConfig(filename='ic3.log',
                        filemode='a',
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.info("Successfully initialized logging")

    def make_dir(self, dir):
     os.makedirs(dir, exist_ok=True)

    def check_data_quality(self, df):
        missing_values = df.isnull().sum().sum()
        # Check for duplicates
        duplicates = df.duplicated().any()
        return missing_values == 0 and not duplicates

    def get_response(self, year, state):
        response = requests.post(self.url.format(year=year), params={'s': f'{state}'})
        soup = BeautifulSoup(response.text, 'html.parser')
        state_name = [tag.text.strip() for tag in soup.find_all('option')][state-1]
        logging.info(f"Url fetched, state: {state_name}")
        return response, state_name

    def write_to_dir(self, dest_table, df, year, state_name):
        output_directory = f'{self.local_folder_path}/{dest_table}/year={year}/state={state_name}'
        self.make_dir(output_directory)
        table = pa.Table.from_pandas(df)
        # Write the partitioned Parquet files
        pq.write_to_dataset(
            table,
            root_path=output_directory,
            compression='snappy',
        )
        logging.info(f"Data has been successfully loaded to the location.: {output_directory}")

    def get_df(self, response, dest_table):
        df = pd.read_html(response.text, header=0, match=f'{self.tables[dest_table]}')[0]
        if dest_table != 'ic3__victims_by_age_group':
            df = df.iloc[:-4]
            df = pd.concat([df.iloc[:, :2], df.iloc[:, -2:].rename(columns={df.columns[2]: df.columns[0], df.columns[3]: df.columns[1]})], axis=0, ignore_index=True)
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        df = df.dropna()
        # Explicitly define schema
        df = df.astype(dtype=self.df_dtype[dest_table])
        logging.info(f"DF created, dest_table: {dest_table}")
        return df

    def main(self):
        self.init_logging()
        try:
            for year in self.years:
                for state in range(1, 57):
                    response, state_name = self.get_response(year, state)
                    for i in range(len(list(self.tables))):
                        dest_table = list(self.tables)[i]
                        df = self.get_df(response, dest_table)
                        data_quality = self.check_data_quality(df)
                        if data_quality:
                            self.write_to_dir(dest_table, df, year, state_name)
                        else:
                            logging.error('Data quality issues.')
        except Exception as e:
            logging.error('An error occurred: %s', str(e))


def load_params_from_json(json_file_path):
    with open(json_file_path, "r") as json_file:
        job_files = json.load(json_file)
    return job_files

if __name__=="__main__": 
    current_directory = os.getcwd()
    params_path = f'{current_directory}/params.json'
    params_json = load_params_from_json(params_path)
    internetCrimeComplaintLoad = InternetCrimeComplaintLoad(
        url = params_json['jobs']['ic3']['url'],
        local_folder_path = params_json['jobs']['ic3']['local_folder_path'],
        tables = params_json['jobs']['ic3']['tables'],
        years = params_json['jobs']['ic3']['years'],
        df_dtype = params_json['jobs']['ic3']['dtype']
    ).main()
