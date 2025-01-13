"""
source_filter.py
This script filters the source data according to specified criteria and saves the filtered data as a Parquet file.
It ensures that only relevant data is retained for further processing.
"""

import logging
import time

start_time = time.time()
logging.warning("Started Source Filtering.....\n")

global ssh_exception, BytesIO
global pa, pq
global datetime
global create_engine, inspect, decrypt_existing_db_credentials, default_engine_creator
global PLATFORM_FILE_PATH, decrypt_existing_sftp_credentials

# can be removed in computation editor
import paramiko
import pandas as pd
import os
import json

import shutil
from paramiko import ssh_exception
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime 

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import URL
from Core.users.computations.db_credential_encrytion import decrypt_existing_db_credentials
from Core.users.computations.db_centralised_function import default_engine_creator 
from config.settings.base import PLATFORM_FILE_PATH
from Core.users.computations.sftp_credential_encryption import decrypt_existing_sftp_credentials

global get_db_connection
def get_db_connection(database, location):
    """
    Parameters:
    - database (str): The name of the database to connect to.
    - location (str): The location of the database (INTERNAL or EXTERNAL).

    Returns:
    - engine: SQLAlchemy engine object.
    - connection: SQLAlchemy connection object.
    - schema: The schema of the database.
    - inspector: SQLAlchemy inspector object.
    """
    
    logging.warning(f"Establishing {location} DB connection for: {database}")

    if location == "INTERNAL":
        db_creds_path = f'{PLATFORM_FILE_PATH}user_databases.json'
    else:
        db_creds_path = f'{PLATFORM_FILE_PATH}external_databases.json'
  
    # Connection to DB starts  
    try:
        with open(db_creds_path, 'r') as file:
            db_json = json.load(file)

        db_data = db_json[database]
        if db_data is None:
            # Terminate if DB not found in system
            logging.warning("Database creds not found in system. Terminating the execution")
            return None, None, None, None
            
        db_type = db_data["db_type"]
        if db_type not in ["MSSQL", "PostgreSQL", "Oracle"]:
            logging.warning("db type should be ['MSSQL', 'PostgreSQL', 'Oracle']")
            return None, None, None, None
    
        logging.warning(f"DB Creds loaded for: {database}")
    except Exception as e:
        logging.warning(f"Error loading {location} db creds for: {database}: {e}")
        return None, None, None, None

    try:
        logging.warning("Decrypting the creds....")
        db_server, port, db_name, username, password = (
            decrypt_existing_db_credentials(
                db_data["server"],
                db_data["port"],
                db_data["db_name"],
                db_data["username"],
                db_data["password"],
                db_data["connection_code"],
            )
        )
        logging.warning(f"Connecting to Database: {db_data['db_name']}")

        global schema
        schema = db_data.get("schema", None)
        db_data["server"] = db_server
        db_data["port"] = port
        db_data["db_name"] = db_name
        db_data["username"] = username
        db_data["password"] = password
        
        global engine, inspector, connection
        if db_type in ["MSSQL", "Oracle"]:
            engine = default_engine_creator(db_type=db_type, connection_details=db_data, return_pg_conn=False)
        else:
            connection_url = URL.create(
                "postgresql+psycopg2",
                username=db_data["username"],
                password=db_data["password"],
                host=db_data["server"],
                port=db_data["port"],
                database=db_data["db_name"]
            )
            engine = create_engine(connection_url)

        connection = engine.connect()
        inspector = inspect(engine)
        logging.warning(f"Successfully connected to Database: {db_data['db_name']}")
        return engine, connection, schema, inspector
    except Exception as e:
        logging.warning(f"Error in connecting to Database:{e}")
        return None, None, None, None


global get_sftp_connection
def get_sftp_connection(Sftp_Name):
    """
    Parameters:
    - Sftp_Name (str): The name of the SFTP server to connect to.

    Returns:
    - sftp_client: Paramiko SFTP client object.
    - ssh_client: Paramiko SSH client object.
    """

    sftp_file_path = f"{PLATFORM_FILE_PATH}sftp_remote_info.json"

    try:
        if os.path.exists(f"{sftp_file_path}"):
            with open(f"{sftp_file_path}", "r+") as fout:
                try:
                    content = json.load(fout)
                    encrypted_hostname = content[Sftp_Name]["hostname"]
                    encrypted_username = content[Sftp_Name]["username"]
                    encrypted_user_secret_key = content[Sftp_Name]["user_secret_key"]
                    connection_code = content[Sftp_Name]["connection_code"]
                    encrypted_port = content[Sftp_Name].get("port", "")
                    fout.close()
                except KeyError as e:
                    logging.warning(f"Error in loading sftp creds: {e}")
                    return None, None
                except Exception as e:
                    logging.warning(f"Error in loading sftp creds: {e}")
                    return None, None
            logging.warning(f"SFTP Creds loaded for: {Sftp_Name}")
        else:
            logging.warning("Sftp json file path changed or does not exist.")
            return None, None
        
        hostname, username, user_secret_key, port = decrypt_existing_sftp_credentials(encrypted_hostname, encrypted_username, encrypted_user_secret_key, encrypted_port, connection_code)
        
        global sftp_client
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        logging.warning(f"Connecting to the SFTP server: {Sftp_Name}...")
        ssh_client.connect(hostname=hostname, username=username, password=user_secret_key, port=port, banner_timeout=200)
        sftp_client = ssh_client.open_sftp()
        logging.warning("Connected to the SFTP server.")
        return sftp_client, ssh_client
    
    except ssh_exception.AuthenticationException as e:
        logging.warning(f"AuthenticationException Connnecting to the sftp: {Sftp_Name}: {e}")
        return None, None

    except Exception as e:
        logging.warning(f"Error Connnecting to the sftp: {Sftp_Name}: {e}")
        return None, None


global map_datatype
def map_datatype(value):
    """
    Parameters:
    - value (str): The datatype to map.

    Returns:
    - function: The corresponding function to convert the datatype.
    """

    if value == "datetime":
        return pd.to_datetime
    if value == "float":
        return float
    
    return str

global convert_datatype
def convert_datatype(value, type):
    """
    Parameters:
    - value: The value to convert.
    - type (str): The target datatype.

    Returns:
    - The value converted to the target datatype.
    """

    if type == "datetime":
        dt_value = pd.to_datetime(value, errors='coerce')
        if pd.isna(dt_value):
            return None  # Handle NaT values
        return dt_value.strftime('%Y-%m-%d')
    if type == "float":
        return float(value)
    return str(value)


global filter_data
def filter_data(table_name, df, column_name, expected_dtype, condition_type, condition_value):
    """
    Filter Data and returns the filtered df.

    Parameters:
    - table_name (str): The name of the table.
    - df (DataFrame): The dataframe to filter.
    - column_name (str): The column to apply the filter on.
    - expected_dtype (str): The expected datatype of the column.
    - condition_type (str): The type of condition (e.g., 'equal to', 'greater than').
    - condition_value: The value to filter by.

    Returns:
    - DataFrame: The filtered dataframe.
    """
    
    if 'date' in column_name:
        if not pd.isna(condition_value):
            if isinstance(condition_value, str):
                date_obj = datetime.strptime(condition_value, "%Y-%m-%d %H:%M:%S")
                condition_value = date_obj.strftime("%Y-%m-%d")
            else:
                condition_value = condition_value.strftime("%Y-%m-%d")
            df[column_name] = pd.to_datetime(df[column_name], errors='coerce', dayfirst=True).dt.strftime('%Y-%m-%d')
    
    logging.warning(f"table: {table_name} Filtering: {column_name} '{condition_type}'  '{condition_value}'")
        
    # Validate condition_type/condition_value
    if (condition_type is None and condition_value is not None) and (condition_type is None and condition_value is not None) :
        logging.warning("condition_type/condition_value empty")
    
    # Validation 
    elif condition_type and condition_value:
        try:
            try:
                mapped_type = map_datatype(expected_dtype)
                converted_condition_value = convert_datatype(condition_value, expected_dtype)
            except Exception as e:
                logging.warning(f"Error in condition_value conversion{e}")
                return
            
            if condition_type == 'equal to':
                df = df[df[column_name].astype(mapped_type) == converted_condition_value]
            elif condition_type == 'not equal to':
                df = df[df[column_name].astype(mapped_type) != converted_condition_value]
            elif condition_type == 'greater than':
                df = df[df[column_name].astype(mapped_type) > converted_condition_value]
            elif condition_type == 'greater than equal to':
                df = df[df[column_name].astype(mapped_type) >= converted_condition_value]
            elif condition_type == 'less than':
                df = df[df[column_name].astype(mapped_type) < converted_condition_value]
            elif condition_type == 'less than equal to':
                df = df[df[column_name].astype(mapped_type) <= converted_condition_value]
            else:
                logging.warning("Unknown type of condition_type")
        except TypeError as e:
            logging.warning(f"Error while filtering data: {e}")
    else:
        logging.warning("condition_type and condition_value are empty")
        
    logging.warning(f"Table: {table_name} filtered with: {column_name} '{condition_type}'  '{condition_value}'")
    return df


global extract_and_apply_filters
def extract_and_apply_filters(table_name, df, filters):
    """
    It filters the table and returns the dataframe.

    Parameters:
    - table_name (str): The name of the table.
    - df (DataFrame): The dataframe to filter.
    - filters (DataFrame): The filters to apply.

    Returns:
    - None: The filtered dataframe is stored in the global variable all_filtered_df.
    """
    
    # Iterate over each filter in each table in each connection_name
    for i, filter in filters.iterrows():
        logging.warning(f"Filtering row:{i+2}")
 
        column_name = filter['source_filter_column_name']
        expected_dtype = filter['condition_datatype']
        target_table_name = filter['source_table_name']

        if column_name is not None:
            if expected_dtype is not None:
                if target_table_name is not None:
                    column_name, expected_dtype, target_table_name = column_name.strip(), expected_dtype.lower().strip(), target_table_name.strip()
                else:
                    logging.warning("Source Table Name empty, Skipping filter and original data will be saved")
                    all_filtered_df[target_table_name] = df
                    continue
            else:
                logging.warning("Column Data Type is empty, Skipping filter and original data will be saved")
                all_filtered_df[target_table_name] = df
                continue
        else:
            logging.warning("Column Name is empty, Skipping filter and original data will be saved")
            all_filtered_df[target_table_name] = df
            continue

        if column_name not in df.columns:
            logging.warning(f"Column does not exist: {column_name}")
            all_filtered_df[target_table_name] = df
            continue

        # filters
        condition_type = filter['condition_type'].strip().lower() if isinstance(filter['condition_type'], str) else filter['condition_type']
        condition_global_var = filter['condition_value']
        condition_value_row = global_params.loc[global_params['column_name'].str.lower().str.strip() == condition_global_var, 'value']
        
        if condition_value_row is not None:
            condition_value = condition_value_row.iloc[0]
        else:
            logging.warning(f"condition_value: {condition_global_var} not present in global parameters")
            all_filtered_df[target_table_name] = df
            continue
          
        df = filter_data(table_name, df, column_name, expected_dtype, condition_type, condition_value)
        logging.warning("   ")
    
    # at the end of loop add filtered df to all_filtered_df
    all_filtered_df[target_table_name] = df
    logging.warning(f"Filtered table: {table_name}")
    

global process_db_filters
def process_db_filters(all_db_filters, location):
    """
    Process all db filters.

    Parameters:
    - all_db_filters (DataFrame): The dataframe containing all the filters.
    - location (str): The location of the database (INTERNAL or EXTERNAL).

    Returns:
    - None: The filtered data is stored in the global variable all_filtered_df.
    """
 
    # Iterate over each connection_name
    for connection_name, db_group in all_db_filters:
        logging.warning(f"Source_location: {connection_name}")

        engine, connection, schema, inspector = get_db_connection(connection_name, location)

        # if engine is None, skip
        if engine is None:
            logging.warning(f"Error in connection for connection_name: {connection_name}")
            continue 
         
        table_group = db_group.groupby("source_name")

        # Iterate over each table
        for table, filters in table_group:
            
            logging.warning(f"Reading {table}")
            table_name = table.lower()

            try:
                # Read table from db
                df = pd.read_sql_table(table_name, engine, schema=schema)
                logging.warning(f"Table loaded: {table}")
            except Exception as e:
                logging.warning(f"Error in Reading table: {table_name}: {e}")
                continue

            extract_and_apply_filters(table_name, df, filters)

            logging.warning("   ")

        connection.close()
        engine.dispose()
        logging.warning(f"Done for {connection_name}")
        logging.warning("   ")

               
global process_sftp_filters
def process_sftp_filters(all_sftp_filters):
    """
    Process all SFTP filters.

    Parameters:
    - all_sftp_filters (DataFrame): The dataframe containing all the SFTP filters.

    Returns:
    - None: The filtered data is stored in the global variable all_filtered_df.
    """

    # Iterate over each connection_name
    for connection_name, sftp_group in all_sftp_filters:
        logging.warning(f"Source Location: {connection_name}")
        sftp_client, ssh_client = get_sftp_connection(connection_name.strip())
        
        # if sftp_client is none, skip
        if sftp_client is None:
            logging.warning(f"Skipped Filtering for connection_name: {connection_name}")
            return 

        file_group = sftp_group.groupby("source_name")
        
        logging.warning("    ")
        # logging.warning(f"length of file group: {file_group.ngroups}")
        
        # Iterate over each file
        for file, filters in file_group:
          
            path = filters.loc[filters['source_name'] == file, 'source_connection_location'].iloc[0]
            file_path = os.path.join(path, file)

            _, file_extension = os.path.splitext(file_path)

            logging.warning(f"Reading {file}")
            # File read
            try:    
                with sftp_client.file(file_path, 'rb') as remote_file:
                    file_data = remote_file.read()
                
                file_buffer = BytesIO(file_data)
                file_buffer.seek(0)

                if file_extension in ['.xlsx', '.xls']:
                    df = pd.read_excel(file_buffer)
                elif file_extension == '.csv':
                    df = pd.read_csv(file_buffer)
                else:
                    logging.warning(f"Unknown file extension, skipped the {file}")
                    continue
    
                logging.warning("File read successfully")
                logging.warning("   ")

            except FileNotFoundError as e:
                logging.warning(f"File not found: {e}, skipped the {file}")
                continue
            except Exception as e:
                logging.warning(f"Exception in sftp: {e}, skipped the {file}")
                continue

            logging.warning(f"No of filter in {file}: {len(filters)} ")

            extract_and_apply_filters(file, df, filters)

            # write df to local
            # df.to_excel(f"{target_table_name}.xlsx", index = False)
            # logging.warning(f"Done for {file}, file:{target_table_name}.xlsx saved")
            logging.warning("    ")
        
        ssh_client.close()
        sftp_client.close()
        logging.warning(f"Done for {connection_name}")
        logging.warning("   ")


global filter_source
def filter_source(all_filters):
    """
    Apply filters to the source data.

    Parameters:
    - all_filters (DataFrame): The dataframe containing all the filters.

    Returns:
    - None: The filtered data is stored in the global variable all_filtered_df.
    """
    
    global all_filtered_df
    all_filtered_df = {}

    # Drop rows where 'source_connection_type', 'source_table_name', 'source_column_name', or 'source_location' are empty
    all_filters = all_filters.copy()
    all_filters = all_filters.dropna(subset=['source_name', 'source_connection_type', 'source_data_type', 'connection_name'])

    try:
        # Filter By source_connection_type, and group by source_location
        internal_db_filters_to_perform = all_filters[
            (all_filters['source_connection_type'].astype(str).str.upper() == "DATABASE") &
            (all_filters['source_connection_location'].astype(str).str.upper() == "INTERNAL")
        ].groupby('connection_name')
        external_db_filters_to_perform = all_filters[
            (all_filters['source_connection_type'].astype(str).str.upper() == "DATABASE") & 
            (all_filters['source_connection_location'].astype(str).str.upper() == "EXTERNAL")
        ].groupby('connection_name')
        sftp_filters_to_perform = all_filters[all_filters['source_connection_type'].astype(str).str.upper() == "SFTP"].groupby('connection_name')
    except Exception as e:
        logging.warning("Error while groupby and filtering with db/sftp and location")
        return

    if internal_db_filters_to_perform.ngroups:
        logging.warning("Started Internal Database Validation")
        process_db_filters(internal_db_filters_to_perform, 'INTERNAL')
        logging.warning("Completed Internal Database Validation")
        logging.warning("   ")
        logging.warning("   ")

    if external_db_filters_to_perform.ngroups:
        logging.warning("Started External Database Validation")
        process_db_filters(external_db_filters_to_perform, 'EXTERNAL')
        logging.warning("Completed External Database Validation")
        logging.warning("    ")
        logging.warning("    ")

    if sftp_filters_to_perform.ngroups:
        logging.warning("Started Sftp Validation")
        process_sftp_filters(sftp_filters_to_perform)
        logging.warning("Completed Sftp Validation")
        logging.warning("   ")
        logging.warning("   ")

    logging.warning(f"Saving file to parquet dir: {parquet_raw_path}")
    # Save filtered data to parquet
    
    if all_filtered_df:
        for table_name, temp_df in all_filtered_df.items():
            try:
                # save to Raw Data Directory
                df_save_path_parquet = os.path.join(parquet_raw_path, f"{table_name}.parquet")
                temp_table = pa.Table.from_pandas(temp_df)
                pq.write_table(temp_table, df_save_path_parquet)

                # save to local for testing
                # output_file = fr"/home/azureuser/DataFrameWork Testing/Filtered Source/{table_name}.xlsx"
                # excel_buffer = BytesIO()
                # temp_df.excel(excel_buffer, index=False)
                # excel_buffer.seek(0)
                # sftp_client.putfo(excel_buffer, output_file)
                logging.warning(f"Parquet saved for {table_name}, {df_save_path_parquet}")
            except Exception as e:
                logging.warning(f"Error in saving parquet for {table_name}, {df_save_path_parquet}")
    else:
        logging.warning("No data to be saved.")


# Main Code
global config_file_path
config_file_path = Config_File_Path

global parquet_path, parquet_raw_path
parquet_path = r"/opt/revolutio/Platform_Configs/alm_data/Data Framework"
parquet_raw_path = os.path.join(parquet_path, "Raw Data")

# Create main directories if not exist, remove raw data directory if exist
try:
    os.makedirs(parquet_path, exist_ok=True)
    shutil.rmtree(parquet_raw_path)
    os.makedirs(parquet_raw_path)   
except FileNotFoundError:
    os.makedirs(parquet_raw_path)    
except OSError as e:
    logging.warning(f"Error deleting Raw Data directory: {e}")

sftp_creds = Data1

# if Sftp_Name is None or Sftp_Name == "":
#     logging.warning("Terminating because sftp name not found")
#     raise Exception("Terminating because sftp name not found")
  
try:
    # filtered_sftp_df =sftp_creds[sftp_creds['variable_name'] == 'Sftp Name'].reset_index(drop=True)
    # index = filtered_sftp_df[filtered_sftp_df['variable_value'] == Sftp_Name].index[0]
    
    hostname = sftp_creds.loc[sftp_creds["variable_name"] == "Hostname", "variable_value"].iloc[0]
    username = sftp_creds.loc[sftp_creds["variable_name"] == "Username", "variable_value"].iloc[0]
    user_secret_key = sftp_creds.loc[sftp_creds["variable_name"] == "Password", "variable_value"].iloc[0]
    port = sftp_creds.loc[sftp_creds["variable_name"] == "Port", "variable_value"].iloc[0]
    
    global sftp_client
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    logging.warning(f"Connecting to the SFTP server: {username}...")
    ssh_client.connect(hostname=hostname, username=username, password=user_secret_key, port=port, banner_timeout=200)
    sftp_client = ssh_client.open_sftp()
    logging.warning("Connected to the SFTP server.")

except ssh_exception.AuthenticationException as e:
    logging.warning(f"AuthenticationException Connnecting to the sftp: {username}: {e}")

except Exception as e:
    logging.warning(f"Error Connnecting to the sftp: {username}: {e}")

# Read Antares Master File
try:
    with sftp_client.file(config_file_path, 'rb') as remote_file:
        file_data = remote_file.read()
    master_file_buffer = BytesIO(file_data)
except FileNotFoundError:
    raise Exception(f"File not found:{config_file_path}")

# Source Data definition
source_data_definition = Data2.copy()

global config_file, master_repository, global_params
try:
    config_file = pd.ExcelFile(master_file_buffer)
    global_params = config_file.parse('global_parameter')
except Exception as e:
    raise Exception(f"Error reading configs file/global parameter sheet: {e}")

global_params.columns = global_params.columns.str.replace(" ","_").str.lower()

logging.warning(f"All config sheets are loaded")

logging.warning("   ")
logging.warning("Started processing source_data_definition")
filter_source(source_data_definition)
logging.warning("Ended processing source_data_definition")

# Output status of data is filtered or not
file_names = source_data_definition['source_table_name'].unique()
file_status = [file_name in all_filtered_df.keys() for file_name in file_names]

output_data = pd.DataFrame({
    "Table Name": file_names,
    "Filter Status": file_status
})
output_data["id"] = output_data.index + 1
output_data.insert(0, 'id', output_data.pop('id'))

ssh_client.close()
sftp_client.close()
logging.warning("Source Filter Completed.")