"""
data_validation.py
This script is responsible for validating raw data to ensure it meets the required standards and formats.
It generates a Data Quality (DQ) Report that highlights any discrepancies or issues found during the validation process.
"""

global create_engine, inspect
global paramiko, ssh_exception, BytesIO
global datetime, pytz
global pa, pq

import logging
import pandas as pd
import logging
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import URL
import os
from io import BytesIO
import paramiko
from paramiko import ssh_exception
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
import pytz

global log_error
def log_error(source_table_name=None, 
              check_type=None, error_reporting_tag_value=None, unique_identifier=None, dq_column_name=None, 
              error_message=None, error_type=None):
    """
    Log the error in validation_error dictionary.

    Parameters:
    - source_table_name (str): Name of the source table.
    - check_type (str): Type of check being performed.
    - error_reporting_tag_value (str): Tag value for error reporting.
    - unique_identifier (str): Unique identifier for the record.
    - dq_column_name (str): Name of the column being validated.
    - error_message (str): Error message to log.
    - error_type (str): Type of error.
    """
    validation_error['extract_date'].append(extract_date)
    validation_error['dq_run_date'].append(dq_run_date)
    validation_error['source_table_name'].append(source_table_name)
    validation_error['check_type'].append(check_type)
    validation_error['error_reporting_tag'].append(error_reporting_tag_value)
    validation_error['unique_identifier'].append(unique_identifier)
    validation_error['dq_column_name'].append(dq_column_name)
    validation_error['error_message'].append(error_message)
    validation_error['error_type'].append(error_type)


global map_datatype
def map_datatype(value):
    """
    Map the datatype to the corresponding conversion function.

    Parameters:
    - value (str): The datatype as a string.

    Expected Output:
    - Function: Corresponding conversion function for the datatype.
    """

    if value == "datetime":
        return pd.to_datetime
    if value == "float":
        return float
    
    return str

global convert_datatype
def convert_datatype(value, type):
    """
    Convert the value into the desired type.

    Parameters:
    - value: The value to be converted.
    - type (str): The target datatype.
    """

    if type == "datetime":
        dt_value = pd.to_datetime(value, errors='coerce')
        if pd.isna(dt_value):
            return None  # Handle NaT values
        return dt_value.strftime('%Y-%m-%d')
    if type == "float":
        return float(value)
    return str(value)


global validate_data
def validate_data(source, df, column_name, expected_dtype, condition_type, condition_value, check_type, unique_identifier_column, error_reporting_tag, error_type, indices_to_drop):
    """
    Validate Data and returns the filtered DataFrame.

    Parameters:
    - source (str): Name of the source table.
    - df (DataFrame): DataFrame containing the data to be validated.
    - column_name (str): Name of the column to be validated.
    - expected_dtype (str): Expected datatype of the column.
    - condition_type (str): Type of condition to validate (e.g., 'equal to', 'greater than').
    - condition_value: Value to be used in the condition for validation.
    - check_type (str): Type of check being performed (e.g., 'null_check', 'unique_check').
    - unique_identifier_column (str): Column name used as a unique identifier for records.
    - error_reporting_tag (str): Tag value for error reporting.
    - error_type (str): Type of error (e.g., 'FATAL').
    - indices_to_drop (set): Set of indices to be dropped in case of FATAL errors.

    Side Effects:
    - Logs errors in the validation_error dictionary.
    - Updates the indices_to_drop set with indices of rows that fail FATAL checks.
    """

    duplicates = pd.DataFrame()
    fails_df = pd.DataFrame()
    new_df = pd.DataFrame()

    if 'date' in column_name:
        if not pd.isna(condition_value):
            if isinstance(condition_value, str):
                date_obj = datetime.strptime(condition_value, "%Y-%m-%d %H:%M:%S")
                condition_value = date_obj.strftime("%Y-%m-%d")
            else:
                condition_value = condition_value.strftime("%Y-%m-%d")
            df[column_name] = pd.to_datetime(df[column_name], errors='coerce', dayfirst=True).dt.strftime('%Y-%m-%d')
    
    logging.warning(f"Validating {check_type}: {column_name} '{condition_type}'  '{condition_value}'")

    t_check_type = check_type.replace(" ", "_").lower()

    if t_check_type == "null_check":
        fails_df = df[(df[column_name].isna()) | (df[column_name] == '')]
        fail_indices = fails_df.index.tolist()

    elif t_check_type == 'unique_check':
        duplicates = df[df.duplicated(subset=[column_name], keep='first')]
        fail_indices = fails_df.index.tolist()

    elif t_check_type == 'master_check':

        all_conditions = list(master_repository.loc[master_repository['master_name'] == condition_value, 'source_column_value'])   
        all_conditions_lower = [str(condition).lower() if isinstance(condition, str) else condition for condition in all_conditions]

        if not all_conditions_lower:
            logging.warning(f"Values for Master Check not defined in Master Repository")
            return

        non_none_types = [type(x) for x in all_conditions_lower if x is not None]
        if not non_none_types:
            logging.warning(f"Values for Master Check not defined in Master Repository")
            return
          
        temp_series = df[column_name].apply(lambda x: x.lower() if isinstance(x, str) else x)
        temp_series = temp_series.astype(non_none_types[0])
      
        fails_df = df[~(temp_series.isin(all_conditions_lower))]
        fail_indices = fails_df.index.tolist()
        condition_value = all_conditions

    # Validate condition_type/condition_value
    elif (condition_type is None and condition_value is not None) and (condition_type is None and condition_value is not None) :
        log_error(source, check_type, error_reporting_tag_value=None, error_message = "condition_type/condition_value is empty")
        logging.warning("condition_type/condition_value empty")
        return

    # Validation 
    elif condition_type and condition_value:
        try: 
            try:
                mapped_type = map_datatype(expected_dtype)
                converted_condition_value = convert_datatype(condition_value, expected_dtype)
            except Exception as e:
                logging.warning(f"{e}")
                log_error(source, check_type, error_reporting_tag_value=None, unique_identifier=None, dq_column_name=column_name, error_message = f"Invalid Condition Value: {condition_value}, expected: {expected_dtype}, got:{str(type(condition_value))}")
                return
            
            if condition_type == 'equal to':
                fails_df = df[df[column_name].astype(mapped_type) == converted_condition_value]
                fail_indices = fails_df.index.tolist()
            elif condition_type == 'not equal to':
                fails_df = df[df[column_name].astype(mapped_type) != converted_condition_value]            
                fail_indices = fails_df.index.tolist()          
            elif condition_type == 'greater than':
                fails_df = df[df[column_name].astype(mapped_type) > converted_condition_value]
                fail_indices = fails_df.index.tolist()
            elif condition_type == 'greater than equal to':
                fails_df = df[df[column_name].astype(mapped_type) >= converted_condition_value]
                fail_indices = fails_df.index.tolist()
            elif condition_type == 'less than':
                fails_df = df[df[column_name].astype(mapped_type) < converted_condition_value]
                fail_indices = fails_df.index.tolist()
            elif condition_type == 'less than equal to':
                fails_df = df[df[column_name].astype(mapped_type) <= converted_condition_value]
                fail_indices = fails_df.index.tolist()
            else:
                logging.warning("Unknown type of condition_type")
                log_error(source, check_type, error_reporting_tag_value=None, unique_identifier=None, dq_column_name=column_name, error_message = "Unknown type of condition_type")
                return
        except TypeError as e:
            logging.warning(f"Error while validating data: {e}")
            log_error(source, check_type, error_reporting_tag_value=None, unique_identifier=None, dq_column_name=column_name, error_message = f"Invalid comparison between {condition_value} and column:{column_name} of type:{df[column_name].dtype}")
            return  

    else:

        log_error(source, check_type, error_reporting_tag_value=None, error_message = "condition_type and condition_value are empty")
        return

    # if fails log the error in report
    if not fails_df.empty:
        for i, row in fails_df.iterrows():
            unique_identifier_value = row[unique_identifier_column]
            actual_condition_value = row[column_name]
            error_reporting_tag_value = row.get(error_reporting_tag, None)
            if t_check_type == "null_check":
                log_error(source, check_type, error_reporting_tag_value, unique_identifier_value, column_name, f"{check_type} failed", error_type)
            else:    
                log_error(source, check_type, error_reporting_tag_value, unique_identifier_value, column_name, f"{check_type} failed: condition:{condition_type} actual:{actual_condition_value} condition_value:{condition_value}", error_type)
    
    # if duplicates found, log in report
    if not duplicates.empty:
        for i,row in duplicates.iterrows():
            unique_identifier_value = row[unique_identifier_column]
            actual_condition_value = row[column_name]
            error_reporting_tag_value = row.get(error_reporting_tag, None)
            log_error(source, check_type, error_reporting_tag_value, unique_identifier_value, column_name, f"{check_type} failed", error_type)
    
    logging.warning("Validated condition_type/condition_value")

    if error_type.upper() == "FATAL": # If FATAL drop, add fail_indices to indices_to_drop
        indices_to_drop.update(fail_indices)


global process_validations
def process_validations(all_validations):
    """
    Process all SFTP validations.

    Parameters:
    - all_validations (DataFrameGroupBy): Grouped DataFrame containing all validations.

    Side Effects:
    - Updates the all_validated_df dictionary with validated DataFrames.
    """
    
    # Iterate over each table
    for file, validations in all_validations:
        
        file_path = os.path.join(parquet_raw_path,f"{file}.parquet")

        # File read
        logging.warning(f"Reading {file}")
        try:    
                        
            if os.path.exists(file_path):
                df = pd.read_parquet(file_path)
            else:
                logging.warning(f"Source data file for '{file}' not found. Skipping this source.")
            
            logging.warning("File read successfully")
            logging.warning("   ")

        except FileNotFoundError as e:
            log_error(file, error_message=f"File not found: {e}, skipped the {file}")
            logging.warning(f"File not found: {e}, skipped the {file}")
            continue

        except Exception as e:
            log_error(file, error_message=f"File not found: {e}, skipped the {file}")
            logging.warning(f"Exception in sftp: {e}, skipped the {file}")
            continue

        logging.warning(f"No of rows to validate {len(validations)} in {file}")

        indices_to_drop = set() # For FATAL error

        # Iterate over each validation in a each table in each system_name
        for i,validation in validations.iterrows():
            logging.warning("   ")
            logging.warning(f"Validating row:{i+2}")
            logging.warning(f"df shape :{df.shape}")

            column_name = validation['source_column_name']
            expected_dtype = validation['condition_datatype']
            unique_identifier_column = validation['unique_identifier']
            error_reporting_tag = validation.get('error_reporting_tag',"")
            
            if column_name is not None:
                if unique_identifier_column is not None:
                    if expected_dtype is not None:
                        column_name, unique_identifier_column, expected_dtype = column_name.strip(), unique_identifier_column.strip(), expected_dtype.lower()
                        if error_reporting_tag is not None:
                            error_reporting_tag = error_reporting_tag.strip()
                    else:
                        logging.warning("Column Datatype is empty, skipping this validation")
                        continue
                else:
                    logging.warning("Unique Identifier Column is empty, skipping this validation")
                    continue
            else:
                logging.warning("Column Name is empty, skipping this validation")
                continue

            # condition to validate
            condition_type = validation['condition_type'].strip().lower() if isinstance(validation['condition_type'], str) else validation['condition_type']
            check_type = validation['check_type'].strip()
            condition_value = validation['master_primary_key_value'] if check_type == "Master Check" else validation['condition_value']

            df.columns = df.columns.str.strip()
            
            if column_name not in df.columns:
                
                log_error(file, check_type, error_reporting_tag_value=None, unique_identifier=None, dq_column_name=column_name, error_message=f"Column does not exist: {column_name}", error_type=error_type)
                logging.warning(f"Column does not exist: {column_name}")
                continue

            validate_data(file, df, column_name, expected_dtype, condition_type, condition_value, check_type, unique_identifier_column, error_reporting_tag, error_type, indices_to_drop)
            logging.warning("   ")

        if indices_to_drop:
            df = df.drop(list(indices_to_drop))
        
        all_validated_df[file] = df
        
        logging.warning(f"Done for {file}, DQ report generated.")
        logging.warning("    ")
    

global validate
def validate(all_checks):
    """
    Validate the checks.

    Parameters:
    - all_checks (DataFrame): DataFrame containing all checks to be validated.

    Side Effects:
    - Updates the all_validated_df dictionary with validated DataFrames.
    """
    
    global all_validated_df
    all_validated_df = {}
    # Drop rows where 'source_connection_type', 'source_table_name', 'source_column_name', or 'source_location' are empty
    all_checks = all_checks.copy()
    
    try:
        all_checks = all_checks.dropna(subset=['source_table_name', 'source_column_name'])
    except KeyError as e:
        logging.warning("No relevant data found for validation in source_data_validation table")
        return
    
    # Filter By source_connection_type, and group by source_location
    validations_to_perform = all_checks.groupby('source_table_name')

    logging.warning("    ")
  
    if validations_to_perform.ngroups:
        logging.warning("Started Sftp Validation")
        process_validations(validations_to_perform)
        logging.warning("Completed Sftp Validation")
    
    if all_validated_df:
        for table_name, temp_df in all_validated_df.items():
            try:
                # save to Raw Data Directory
                df_save_path_parquet = os.path.join(parquet_error_free_path,f"{table_name}.parquet")
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
global config_file_path, raw_data_dir
config_file_path = Config_File_Path

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
    raise Exception(f"Error Connnecting to the sftp: {username}: {e}")

# Read Antares Master File
try:
    with sftp_client.file(config_file_path, 'rb') as remote_file:
        file_data = remote_file.read()
    master_file_buffer = BytesIO(file_data)
except FileNotFoundError:
    raise Exception(f"File not found:{config_file_path}")

global dq_run_date
dq_run_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M')

global parquet_path, parquet_raw_path, parquet_error_free_path
parquet_path = r"/opt/revolutio/Platform_Configs/alm_data/Data Framework"
parquet_raw_path = os.path.join(parquet_path, "Raw Data")
parquet_error_free_path = os.path.join(parquet_path, "Error Free Data")

# Create directories if not exist
if not os.path.exists(parquet_raw_path):
    raise Exception("No Raw Data found")

try:
    shutil.rmtree(parquet_error_free_path)
    os.makedirs(parquet_error_free_path)   
except FileNotFoundError:
    os.makedirs(parquet_error_free_path)    
except OSError as e:
    logging.warning(f"Error deleting Raw Data directory: {e}")

os.makedirs(parquet_error_free_path, exist_ok=True)

global config_file, master_repository

# Validation, Master Repository 
source_data_validation = Data2
master_repository = Data3

# Read necessary sheets
try:
    config_file = pd.ExcelFile(master_file_buffer)
    global_params = config_file.parse('global_parameter')
except Exception as e:
    raise Exception(f"Error reading config file/global parameter sheet:{e}")

logging.warning(f"All config sheets are loaded")

# Read extract_date to log in DQ report
global extract_date
try:
    extract_date = global_params.loc[global_params['column_name'].str.lower().str.strip() == "reporting_date", 'value'].iloc[0].strftime("%Y-%m-%d")
except IndexError as e:
    extract_date = ""

global validation_error
validation_error = {
    'extract_date': [], 'dq_run_date': [], 'source_table_name': [],
    'check_type': [], 'error_reporting_tag': [], 'unique_identifier': [], 'dq_column_name': [],
    'error_message': [], 'error_type': []
}
logging.warning("    ")

# Processing source_data_validation
logging.warning("Started processing source_data_validation")
validate(source_data_validation)
logging.warning("Ended processing source_data_validation")

# save validation file
validation_error_df = pd.DataFrame(validation_error)

# Validation completed

file_names = [os.path.splitext(i)[0] for i in os.listdir(parquet_raw_path)]
file_status = [file_name in all_validated_df.keys() for file_name in file_names]
output_data = pd.DataFrame({
    "Table Name": file_names,
    "Validation Status": file_status
})
output_data["id"] = output_data.index + 1
output_data.insert(0, 'id', output_data.pop('id'))

ssh_client.close()
sftp_client.close()
logging.warning("   ")
logging.warning("Validation Completed")
