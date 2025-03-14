# Data_insertation_Project_PhonePe

This project processes JSON data and inserts the processed information into a Microsoft SQL Server database. It involves processing different types of data such as insurance transactions, user transactions, and state-specific insurance data. The project is structured to handle large data efficiently and insert it into the corresponding database tables.

## Requirements

- Python 3.x
- Required libraries:
  - `pandas`
  - `pyodbc`
  - `sqlalchemy`
  - `json`
  - `os`
  - `logging`

## Setup

1. Install required dependencies:

```bash
pip install pandas pyodbc sqlalchemy
Ensure that the ODBC Driver 17 for SQL Server is installed and configured on your machine.

Update the configuration section of the code as needed:

SERVER: The name of your SQL Server instance.
DATABASE: The name of the database.
TRUSTED_CONNECTION: Set this to 'yes' if you're using Windows Authentication.
DATA_DIR: The directory path containing the aggregated JSON data files.
Structure
The main components of the project are:

Logging Configuration: The project uses logging to track the execution flow and errors.

Database Connection: The connection to SQL Server is handled using pyodbc and sqlalchemy. The create_engine function establishes the connection.

Data Processing Functions:

truncate_tables(): Truncates all tables before inserting new data.
process_json(filepath, extract_func): Loads JSON data from the file and applies an extraction function to process the data.
insert_df(df, table_name): Inserts the processed DataFrame into the specified table in the database.
process_insurance(data), process_transaction(data), process_user(data): Functions for processing insurance transactions, general transactions, and user data respectively.
process_state_insurance(filepath): Processes insurance data specific to a state.
process_dir(directory, process_func, table_name): Iterates through all JSON files in the directory and processes them with the specified function, inserting the data into the specified table.
State-Specific Processing: A list of states is used to iterate through the insurance data directories for each state. The data is processed and inserted into a dedicated states_Insurance table.

Execution Flow
Truncate Tables: The truncate_tables() function is called to clear out any existing data from the relevant tables before inserting the new data.

Process Directories: The process_dir() function is used to iterate over directories containing JSON files (insurance, transaction, and user) and process them with the respective functions (process_insurance, process_transaction, process_user).

State-Specific Insurance Processing: A list of states is defined. The state-specific insurance data is processed for each state and inserted into the states_Insurance table.

Example Usage
Once the configuration is updated and the required libraries are installed, running the script will:

Truncate the relevant database tables.
Process the JSON data in the specified directories.
Insert the processed data into the corresponding database tables (InsuranceTransactions, TransactionData, UserData, states_Insurance).
Notes
Ensure that the directory paths for DATA_DIR and the insurance directories are correct.
The script assumes that the structure of the JSON data is consistent with the expected format. If there are discrepancies, the processing functions will log warnings or errors.
The script uses pandas for handling and inserting data into the SQL Server database, making it suitable for large datasets.
Streamlit Integration (Optional)
There is an optional section in the code (commented out) that integrates with Streamlit for visualization of the processed data. If you wish to use Streamlit, ensure that you have it installed by running:

bash
Copy
pip install streamlit
You can enable the Streamlit integration by uncommenting the related sections in the code.

License
This project is licensed under the MIT License - see the LICENSE file for details.

pgsql
Copy

This `README.md` file will give users clear guidance on setting up and running the project, including installation requirements, configuration, and usage details.



