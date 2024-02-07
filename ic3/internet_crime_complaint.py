import json
import logging
import os
from typing import Any, Dict, List, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup

class InternetCrimeComplaintLoad:
    def __init__(
        self,
        url: str,
        local_folder_path: str,
        tables: Dict[str, str],
        years: List[int],
        dtype_json: Dict[str, Any],
    ) -> None:
        self.url = url
        self.local_folder_path = local_folder_path
        self.tables = tables
        self.years = years
        self.dtype_json = dtype_json

    def init_logging(self) -> None:
        logging.basicConfig(
            filename="ic3.log",
            filemode="a",
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        logging.info("Successfully initialized logging")

    @staticmethod
    def make_directory(dir: str) -> None:
        os.makedirs(dir, exist_ok=True)
    
    @staticmethod
    def check_data_quality(df: pd.DataFrame) -> bool:
        missing_values = df.isnull().sum().sum()
        # Check for duplicates
        duplicates = df.duplicated().any()
        return missing_values == 0 and not duplicates
    
    
    def get_response(self, year: int, state: int) -> Tuple[requests.Response, str]:
        try:
            response = requests.post(
                self.url.format(year=year), params={"s": f"{state}"}, timeout=10
            )
        except requests.exceptions.Timeout:
            print("Timed out")

        soup = BeautifulSoup(response.text, "html.parser")
        state_name = [tag.text.strip() for tag in soup.find_all("option")][state - 1]
        logging.info(f"Url fetched, state: {state_name}")
        return response, state_name

    def write_to_directory(
        self, dest_table: str, df: pd.DataFrame, year: int, state_name: str
    ) -> None:
        output_directory = (
            f"{self.local_folder_path}/{dest_table}/year={year}/state={state_name}"
        )
        self.make_directory(output_directory)
        table = pa.Table.from_pandas(df)
        # Write the partitioned Parquet files
        pq.write_to_dataset(
            table,
            root_path=output_directory,
            compression="snappy",
        )
        logging.info(f"Data has been successfully loaded to the location.: {output_directory}")

    def get_df(self, response, dest_table):
        df_reports = pd.read_html(response.text, header=0, match=f'{self.tables[dest_table]}')[0]
        if dest_table != 'ic3__victims_by_age_group':
            df_reports = df_reports.iloc[:-4]
            df_reports = pd.concat([df_reports.iloc[:, :2], df_reports.iloc[:, -2:].rename(columns={df_reports.columns[2]: df_reports.columns[0], df_reports.columns[3]: df_reports.columns[1]})], axis=0, ignore_index=True)
        df_reports.columns = df_reports.columns.str.lower().str.replace(' ', '_')
        df_reports = df_reports.dropna()
        # Explicitly define schema
        df_reports = df_reports.astype(dtype=self.dtype_json[dest_table])
        logging.info(f"DF created, dest_table: {dest_table}")
        return df_reports
    
    def get_df(self, response: requests.Response, dest_table: str) -> pd.DataFrame:
        df_reports = pd.read_html(
            response.text, header=0, match=f"{self.tables[dest_table]}"
        )[0]
        if dest_table != "ic3__victims_by_age_group":
            df_reports = df_reports.iloc[:-4]
            df_reports = pd.concat(
                [
                    df_reports.iloc[:, :2],
                    df_reports.iloc[:, -2:].rename(
                        columns={
                            df_reports.columns[2]: df_reports.columns[0],
                            df_reports.columns[3]: df_reports.columns[1],
                        }
                    ),
                ],
                axis=0,
                ignore_index=True,
            )
        df_reports.columns = df_reports.columns.str.lower().str.replace(" ", "_")
        df_reports = df_reports.dropna()
        # Explicitly define schema
        df_reports = df_reports.astype(dtype=self.dtype_json[dest_table])
        logging.info(f"DF created, dest_table: {dest_table}")
        return df_reports

    def main(self) -> None:
        self.init_logging()
        try:
            for year in self.years:
                for state in range(1, 57):
                    response, state_name = self.get_response(year, state)
                    for i in range(len(list(self.tables))):
                        dest_table = list(self.tables)[i]
                        df_reports = self.get_df(response, dest_table)
                        data_quality = self.check_data_quality(df_reports)
                        if data_quality:
                            self.write_to_directory(dest_table, df_reports, year, state_name)
                        else:
                            logging.error("Data quality issues.")
        except Exception as e:
            logging.error("An error occurred: %s", str(e))

def load_params_from_json(json_file_path: str) -> Dict[str, Any]:
    with open(json_file_path, "r") as json_file:
        job_files = json.load(json_file)
    return job_files


if __name__ == "__main__":
    current_directory = os.getcwd()
    params_path = f"{current_directory}/params.json"
    params_json = load_params_from_json(params_path)
    internetCrimeComplaintLoad = InternetCrimeComplaintLoad(
        url=params_json["jobs"]["ic3"]["url"],
        local_folder_path=params_json["jobs"]["ic3"]["local_folder_path"],
        tables=params_json["jobs"]["ic3"]["tables"],
        years=params_json["jobs"]["ic3"]["years"],
        dtype_json=params_json["jobs"]["ic3"]["dtype"],
    ).main()
