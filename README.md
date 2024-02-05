Internet Crime Complaint Load
Overview
The Internet Crime Complaint Load is a Python script designed to fetch and process internet crime complaint data from a specified URL, transforming it into a structured form and storing it in Parquet files. The script utilizes various Python libraries, including requests, pandas, beautifulsoup4, pyarrow, and json. It is configured using a JSON parameter file, allowing easy customization of input parameters.

Requirements
Python 3.6 or later
Required Python libraries: requests, pandas, beautifulsoup4, pyarrow
Usage
Clone the repository:
git clone https://github.com/your-username/your-repository.git
cd your-repository

Install the required dependencies:
pip install -r requirements.txt

Please edit a JSON parameter file params.json with the necessary configuration. local_folder_path should be replaced with the correct one.

url: URL template for fetching data.
local_folder_path: Local directory to store the processed data.
tables: Mapping of destination tables to the header strings to identify them in the HTML.
years: List of years for which data should be fetched.
df_dtype: Data types for columns in each destination table.

Logging
The script utilizes logging to capture important events and errors. The log file (ic3.log) is created in the script's directory.

Exception Handling
The script handles exceptions gracefully and logs errors with relevant information.

Feel free to customize this documentation according to your specific needs and provide additional information as necessary.
