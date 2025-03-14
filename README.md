import os
import json
import pandas as pd
import pyodbc
import logging
from sqlalchemy import create_engine


# Config (adjust as needed)
LOG_LEVEL = logging.INFO
SERVER = 'LENOVO-V130'
DATABASE = 'PhonePE_Project'
TRUSTED_CONNECTION = 'yes'
DATA_DIR = r'C:\Users\User\Desktop\Code Practice\Python-workspace\pulse\data\aggregated'


# Logging  
logging.basicConfig(level=LOG_LEVEL)


# Function to execute the stored procedure to truncate tables
def truncate_tables():
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection={TRUSTED_CONNECTION};'
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    try:
        logging.info("Executing TruncateAllTables stored procedure...")
        cursor.execute("EXEC dbo.TruncateAllTables")
        conn.commit()
        logging.info("Stored procedure executed successfully. All tables truncated.")
    except Exception as e:
        logging.error(f"Error executing stored procedure: {e}")
    finally:
        cursor.close()
        conn.close()


def get_conn_str():
    return f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection={TRUSTED_CONNECTION};'


def get_db_engine():
    return create_engine(f'mssql+pyodbc:///?odbc_connect={get_conn_str()}')


def process_json(filepath, extract_func):
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                return extract_func(data)
            except Exception as e:
                logging.error(f"Error processing {filepath}: {e}")
                return None


def insert_df(df, table_name):
    if df is not None and not df.empty:
        try:
            df.to_sql(table_name, get_db_engine(), if_exists='append', index=False)
            logging.info(f"Data inserted into {table_name}")
        except Exception as e:
            logging.error(f"Error inserting into {table_name}: {e}")


def process_insurance(data):
    if 'data' in data and 'transactionData' in data['data']:
        rows = []
        for trans in data['data']['transactionData']:
            if trans['name'] == 'Insurance':  # Check if transaction is of type 'Insurance'
                if 'paymentInstruments' in trans:  # Ensure 'paymentInstruments' is present
                    for pay in trans['paymentInstruments']:
                        # Safely extract data and handle missing values
                        rows.append({
                            'success': data.get('success'),
                            'code': data.get('code'),
                            'data_from': pd.to_datetime(data['data'].get('from'), unit='ms', errors='coerce'),
                            'data_to': pd.to_datetime(data['data'].get('to'), unit='ms', errors='coerce'),
                            'responseTimestamp': pd.to_datetime(data.get('responseTimestamp'), unit='ms', errors='coerce'),
                            'transaction_name': trans.get('name'),
                            'payment_type': pay.get('type', 'Unknown'),  # Default to 'Unknown' if missing
                            'payment_count': pay.get('count', 0),  # Default to 0 if missing
                            'payment_amount': pay.get('amount', 0.0)  # Default to 0 if missing
                        })
                else:
                    logging.warning(f"Missing 'paymentInstruments' in transaction: {trans}")
            else:
                logging.debug(f"Skipping non-insurance transaction: {trans}")
        if rows:  # Ensure that we only return data if rows have been appended
            return pd.DataFrame(rows)
        else:
            logging.warning(f"No 'Insurance' transactions found in data: {data}")
    else:
        logging.error(f"Missing 'transactionData' or 'data' in the input: {data}")
    return None  # Return None if the data doesn't contain 'transactionData'


def process_transaction(data):
    if 'data' in data and 'transactionData' in data['data']:
        rows = []
        for trans in data['data']['transactionData']:
            for pay in trans['paymentInstruments']:
                rows.append({
                    'success': data.get('success'), 'code': data.get('code'),
                    'data_from': pd.to_datetime(data['data'].get('from'), unit='ms', errors='coerce'),
                    'data_to': pd.to_datetime(data['data'].get('to'), unit='ms', errors='coerce'),
                    'responseTimestamp': pd.to_datetime(data.get('responseTimestamp'), unit='ms', errors='coerce'),
                    'transaction_name': trans.get('name'), 'payment_type': pay.get('type'),
                    'payment_count': pay.get('count'), 'payment_amount': pay.get('amount')
                })
        return pd.DataFrame(rows)


def process_user(data):
    # Check if 'data' exists in the input JSON structure
    if 'data' in data:
        logging.debug(f"Data for processing: {data}")
       
        # Check if 'usersByDevice' exists in the 'data' and is not None
        users_by_device = data['data'].get('usersByDevice')


        # If 'usersByDevice' is None, log the warning and set it to None (as an empty list to process safely)
        if users_by_device is None:
            logging.warning(f"'usersByDevice' is None in data: {data}")
            users_by_device = []  # Treat 'None' as an empty list to process it safely
           
        # If 'usersByDevice' is not a list, log and skip the file
        if not isinstance(users_by_device, list):
            logging.error(f"'usersByDevice' is not a list in data: {data}")
            return None  # Skip this file if 'usersByDevice' is malformed
       
        # Proceed with processing if 'usersByDevice' is a valid list
        rows = []
        agg = data['data'].get('aggregated', {})


        # Ensure that 'aggregated' contains the required fields and log warnings if missing
        if 'registeredUsers' not in agg:
            logging.warning(f"Missing 'registeredUsers' in aggregated data: {data}")
        if 'appOpens' not in agg:
            logging.warning(f"Missing 'appOpens' in aggregated data: {data}")


        # Process the 'usersByDevice' list (if it exists) or insert default row if empty
        if users_by_device:
            for dev in users_by_device:
                if not isinstance(dev, dict):  # Ensure each device entry is a dictionary
                    logging.error(f"Invalid device data format in 'usersByDevice': {dev}")
                    continue  # Skip invalid entries


                rows.append({
                    'success': data.get('success'),
                    'code': data.get('code'),
                    'registeredUsers': agg.get('registeredUsers', 0),  # Default to 0 if missing
                    'appOpens': agg.get('appOpens', 0),  # Default to 0 if missing
                    'responseTimestamp': pd.to_datetime(data.get('responseTimestamp'), unit='ms', errors='coerce'),
                    'device_brand': dev.get('brand', 'Unknown'),  # Default to 'Unknown' if missing
                    'device_count': dev.get('count', 0),  # Default to 0 if missing
                    'device_percentage': dev.get('percentage', 0)  # Default to 0 if missing
                })
        else:
            # If 'usersByDevice' is empty (or None), insert a default row with NULLs for device-related fields
            rows.append({
                'success': data.get('success'),
                'code': data.get('code'),
                'registeredUsers': agg.get('registeredUsers', 0),  # Default to 0 if missing
                'appOpens': agg.get('appOpens', 0),  # Default to 0 if missing
                'responseTimestamp': pd.to_datetime(data.get('responseTimestamp'), unit='ms', errors='coerce'),
                'device_brand': None,  # Set to None when no device data is available
                'device_count': None,  # Set to None when no device data is available
                'device_percentage': None  # Set to None when no device data is available
            })


        # If there are valid rows, return them as a DataFrame
        if rows:
            return pd.DataFrame(rows)
        else:
            logging.warning(f"No valid 'usersByDevice' data found in: {data}")
    else:
        logging.error(f"Missing 'data' in the input JSON: {data}")
   
    return None  # Return None if 'usersByDevice' is missing or empty




def process_state_insurance(filepath):
    data = process_json(filepath, lambda x: x)
    if data and 'data' in data and 'transactionData' in data['data']:
        state = os.path.basename(os.path.dirname(os.path.dirname(filepath)))
        year = os.path.basename(os.path.dirname(filepath))
        rows = []
        for trans in data['data']['transactionData']:
            for pay in trans.get('paymentInstruments', []):
                rows.append({
                    'state_name': state, 'transaction_name': trans.get('name', 'Unknown'),
                    'instrument_type': pay.get('type', 'Unknown'), 'count': pay.get('count', 0),
                    'amount': pay.get('amount', 0.0),
                    'responseTimestamp': pd.to_datetime(data.get('responseTimestamp'), unit='ms', errors='coerce')
                })
        return pd.DataFrame(rows)


def process_dir(directory, process_func, table_name):
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                filepath = os.path.join(root, file)
                df = process_json(filepath, process_func)
                insert_df(df, table_name)


# Main execution
truncate_tables()  # Truncate all tables before inserting new data
process_dir(os.path.join(DATA_DIR, 'insurance'), process_insurance, 'InsuranceTransactions')
process_dir(os.path.join(DATA_DIR, 'transaction'), process_transaction, 'TransactionData')
process_dir(os.path.join(DATA_DIR, 'user'), process_user, 'UserData')


# State-specific processing
states = ['andaman-&-nicobar-islands','andhra-pradesh','arunachal-pradesh','assam','bihar','chandigarh','chhattisgarh','dadra-&-nagar-haveli','daman-&-diu','delhi','goa','gujarat','haryana','himachal-pradesh','jammu-&-kashmir','jharkhand','karnataka','kerala','lakshadweep','madhya-pradesh','maharashtra','manipur','meghalaya','mizoram','nagaland','odisha','puducherry','punjab','rajasthan','sikkim','tamil-nadu','telangana','tripura','uttar-pradesh','uttarakhand','west-bengal']


for state in states:
    state_dir = os.path.join(DATA_DIR, 'insurance', 'country', 'india', 'state', state)
    for root, _, files in os.walk(state_dir):
        for file in files:
            if file.endswith('.json'):
                filepath = os.path.join(root, file)
                df = process_state_insurance(filepath)
                insert_df(df, 'states_Insurance')  
# import streamlit as st
# from sqlalchemy import create_engine
# import pandas as pd


# Function to get database engine
# def get_db_engine():
#     SERVER = 'LENOVO-V130'
#     DATABASE = 'PhonePE_Project'
#     TRUSTED_CONNECTION = 'yes'
#     conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection={TRUSTED_CONNECTION};'
#     return create_engine(f'mssql+pyodbc:///?odbc_connect={conn_str}')


# # Function to execute SQL query and fetch sum of 'count' from states_Insurance
# def get_total_insurance_count():
#     engine = get_db_engine()
#     query = "SELECT SUM([count]) as total_count FROM [dbo].[states_Insurance]"
#     df = pd.read_sql(query, engine)
#     if df is not None and not df.empty:
#         return df['total_count'].iloc[0]
#     else:
#         return 0  # Return 0 if no data is found


# # Function to execute SQL query and fetch total premium sum
# def get_total_premium():
#     engine = get_db_engine()
#     query = "SELECT SUM(amount) as total_premium FROM [dbo].[states_Insurance]"
#     df = pd.read_sql(query, engine)
#     if df is not None and not df.empty:
#         return df['total_premium'].iloc[0]
#     else:
#         return 0  # Return 0 if no data is found


# # Custom CSS for background color
# st.markdown("""
#     <style>
#         .stApp {
#             background-color: #A349A4;  /* PhonePe's purple color */
#             color: white;
#         }
       
#         /* CSS to float the logo image to the top right */
#         .logo-right {
#             position: absolute;
#             top: 10px;
#             right: 10px;
#         }


#         .stButton>button {
#             background-color: #ffffff;
#             color: #A349A4;
#             border-radius: 5px;
#         }


#         .stButton>button:hover {
#             background-color: #E3A8E1;
#             color: white;
#         }
#     </style>
# """, unsafe_allow_html=True)


# # Displaying the logo image at the top-right corner
# st.markdown('<div class="logo-right">', unsafe_allow_html=True)
# st.image(r"C:\Users\User\Desktop\Code Practice\Python-workspace\Image\phonepe_logo.png.jpg", width=150)  # Adjust the width to your preference
# st.markdown('</div>', unsafe_allow_html=True)


# # App title
# st.title("Welcome to PhonePe App")


# # App content
# st.write("This is a simple Streamlit app with a background inspired by the PhonePe logo.")


# # Fetch the sum of insurance policies purchased
# total_count = get_total_insurance_count()


# # Display the total count below the header
# st.subheader("All India Insurance Policies Purchased (Nos.)")
# st.title(f"Total Policies Purchased: {total_count:,.0f}")


# # Get the total premium amount from the database
# total_premium = get_total_premium()


# # Convert total premium amount to Crores (1 crore = 10,000,000)
# total_premium_in_crores = total_premium / 10000000


# # Display the total premium in crores
# st.markdown(f"### Total Premium Value: ₹{total_premium_in_crores:.2f} Crores")




# # Comfiguring Streamlit GUI
# st.set_page_config(
#     page_title="Mr.Paul's Youtube analysis",
#     page_icon="🥷",
#     layout="wide")
# st.balloons()





