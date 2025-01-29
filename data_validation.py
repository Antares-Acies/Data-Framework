"""
data_validation.py
This script is responsible for validating raw data to ensure it meets the required standards and formats.
It generates a Data Quality (DQ) Report that highlights any discrepancies or issues found during the validation process.
"""

global create_engine, inspect
global paramiko, ssh_exception, BytesIO, json, shutil
global pytz
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
import json
global PLATFORM_FILE_PATH, decrypt_existing_sftp_credentials
from config.settings.base import PLATFORM_FILE_PATH
from Core.users.computations.sftp_credential_encryption import decrypt_existing_sftp_credentials

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
        return object
    if value == "float":
        return float
    return str

global format_value
def format_value(value):
    try:
        value = pd.to_datetime(value, errors='coerce').strftime('%Y-%m-%d %H:%M:%S')
        return value
    except ValueError:
        return value
    # if isinstance(value, pd.Timestamp):
    #     logging.warning('called and changed')
    #     return value.strftime('%Y-%m-%d %H:%M:%S')
    # return value

global convert_datatype
def convert_datatype(value, expected_dtype):
    """
    Convert the value into the desired type.

    Parameters:
    - value: The value to be converted.
    - type (str): The target datatype.
    """

    if expected_dtype == "datetime":
        dt_value = pd.to_datetime(value, errors='coerce')
        if pd.isna(dt_value):
            return None  # Handle NaT values
        return dt_value.strftime('%Y-%m-%d %H:%M:%S')
    
    if expected_dtype == "float":
        return float(value)
    
    return str(value)

global is_float
def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

global is_string
def is_string(value):
    return isinstance(value, str)

global is_datetime
def is_datetime(value):
    try:
        pd.to_datetime(value)
        return True
    except (ValueError, TypeError):
        return False

global filter_data
def filter_data(df, filter_details):
    """
    Return filtered df or error_message in case of error
    """

    filter_column, filter_condition_datatype, filter_condition_type, filter_condition_value, filter_value_type = filter_details
    
    if not pd.isna(filter_column):
        filter_column = filter_column.strip()

    if pd.isna(filter_value_type):
        error_msg = f"Filter Value Type is empty, skipping this validation"
        return None, error_msg
      
    if pd.isna(filter_condition_value):
        error_msg = f"Filter Condition Value is empty, skipping this validation"
        return None, error_msg

    logging.warning("Filter Details: ")    
    logging.warning(f"Filter column: {filter_column} Datatype: {filter_condition_datatype}")    
    logging.warning(f"Filter Condition Type: {filter_condition_type} Value Type:{filter_value_type}")    
    logging.warning(f"Filter Condition Value: {filter_condition_value}")    

    filter_value_type = filter_value_type.strip().lower().replace(" ","_")
    filter_condition_value = filter_condition_value.strip() if isinstance(filter_condition_value, str) else filter_condition_value
    filter_condition_type = filter_condition_type.strip().lower()
    filter_condition_datatype = filter_condition_datatype if pd.isna(filter_condition_datatype) else filter_condition_datatype.lower().strip()

    
    if filter_value_type == 'global_variable':
        if filter_condition_value.lower().replace(' ','_') == 'reporting_date':
            g_condition_value = filter_condition_value
            filter_condition_value = reporting_date
            logging.warning(f"condition_value fetched for global_variable: {g_condition_value}: {filter_condition_value}")
        else:
            error_msg = f"Global variable defined:{filter_condition_value} not avaiable in Global Parameters, skipping this validation"
            return None, error_msg

    if filter_value_type == 'mapping_set':
        logging.warning(f"Filtering with Mapping Set: {filter_condition_value}")
        try:
            mapping_conditions_df = mapping_set.loc[mapping_set['mapping_set'] == filter_condition_value]
        except KeyError as e:
            raise Exception(f"here:{mapping_set.columns}")
        if not mapping_conditions_df.empty:
            filtered_df = df
            mapping_conditions_df_group = mapping_conditions_df.groupby('mapping_type')
            for col, mapping_conditions in mapping_conditions_df_group:
                if col in filtered_df.columns:
                    mapping_values = list(mapping_conditions['mapping_criteria'])
                    if filter_condition_type == 'contains':
                        filtered_df = filtered_df[filtered_df[col].isin(mapping_values)]
                    else:
                        filtered_df = filtered_df[~(filtered_df[col].isin(mapping_values))]
                    logging.warning(f"Filter col: {col} with Values: {mapping_values}")
                else:
                    error_msg = f"Columns defined for filter using mapping does not exist in the table, skipping this validation"
                    filtered_df = None
                                        
        else:
            error_msg = f"Mapping set defined for filter not avaiable in Mapping set table, skipping this validation"
            filtered_df = None

    elif filter_value_type == 'column':
        if filter_condition_type == 'equal to' or filter_condition_type == 'contains':
            filtered_df = df[df[filter_column] == df[filter_condition_value]]
        elif filter_condition_type == 'not equal to' or filter_condition_type == 'does not contains':
            filtered_df = df[df[filter_column] != df[filter_condition_value]]
        elif filter_condition_type == 'greater than':
            filtered_df = df[df[filter_column] > df[filter_condition_value]]
        elif filter_condition_type == 'greater than equal to':
            filtered_df = df[df[filter_column] >= df[filter_condition_value]]
        elif filter_condition_type == 'less than':
            filtered_df = df[df[filter_column] < df[filter_condition_value]]
        elif filter_condition_type == 'less than equal to':
            filtered_df = df[df[filter_column] <= df[filter_condition_value]]
        else:
            error_msg = f"Unknown type of filter_condition_type: {filter_condition_type}"
            filtered_df = None

    elif filter_value_type == 'value' or filter_value_type == 'global_variable':
        # Map datatype of column
        if not pd.isna(filter_condition_datatype):
            if filter_condition_datatype.lower() == 'datetime':
                df[filter_column] = pd.to_datetime(df[filter_column])
            if filter_condition_datatype == 'float':
                filter_condition_value = float(filter_condition_value)
                df[filter_column] = df[filter_column].astype(float)
            
        if filter_condition_type == 'equal to' or filter_condition_type == 'contains':
            filtered_df = df[df[filter_column] == filter_condition_value]
        elif filter_condition_type == 'not equal to' or filter_condition_type == 'does not contains':
            filtered_df = df[df[filter_column] != filter_condition_value]
        elif filter_condition_type == 'greater than':
            filtered_df = df[df[filter_column] > filter_condition_value]
        elif filter_condition_type == 'greater than equal to':
            filtered_df = df[df[filter_column] >= filter_condition_value]
        elif filter_condition_type == 'less than':
            filtered_df = df[df[filter_column] < filter_condition_value]
        elif filter_condition_type == 'less than equal to':
            filtered_df = df[df[filter_column] <= filter_condition_value]
        else:
            error_msg = f"Unknown type of filter_condition_type: {filter_condition_type}"
            filtered_df = None

    return filtered_df, None

global validate_data
def validate_data(file, df, filter_details, check_details, master_check_details, check_type, unique_identifier_column, error_reporting_tag, error_type, indices_to_drop):
    """
    Validate Data and returns the filtered DataFrame.

    Parameters:
    - file (str): Name of the table.
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

    # if filter_column is defined then filter the df first
    column_name, condition_datatype, condition_type, condition_value, check_value_type = check_details
    
    filter_column, filter_condition_datatype, filter_condition_type, filter_condition_value, filter_value_type = filter_details
    if not (pd.isna(filter_column) and pd.isna(filter_condition_datatype) and pd.isna(filter_condition_type) and pd.isna(filter_condition_value) and pd.isna(filter_value_type)):
        logging.warning(f"Filtering the df: {len(df)}")
        filtered_df, error_msg = filter_data(df, filter_details)
        if error_msg is not None:
            logging.warning(error_msg)
            log_error(file, check_type, error_reporting_tag, dq_column_name=column_name, error_message = error_msg, error_type=error_type)
            return
        logging.warning(f"Filtered the df: {len(filtered_df)}")
    else:
        filtered_df = df
    
    # variables to hold the duplicates, failed, datatype_failed and fail_indices values
    duplicates = pd.DataFrame()
    fails_df = pd.DataFrame()
    datatype_fails_df = pd.DataFrame()
    fail_indices = None

    # if 'date' in column_name:
    #     if not pd.isna(condition_value):
    #         if isinstance(condition_value, str):
    #             date_obj = datetime.strptime(condition_value, "%Y-%m-%d %H:%M:%S")
    #             condition_value = date_obj.strftime("%Y-%m-%d")
    #         else:
    #             condition_value = condition_value.strftime("%Y-%m-%d")
    #         filtered_df[column_name] = pd.to_datetime(filtered_df[column_name], errors='coerce', dayfirst=True).dt.strftime('%Y-%m-%d')
    
    logging.warning(f"Validating {check_type}: {column_name} '{condition_type}'  '{condition_value}'")

    t_check_type = check_type.replace(" ", "_").lower().strip()

    # For null check
    if t_check_type == "null_check":
        fails_df = filtered_df[(filtered_df[column_name].isna()) | (filtered_df[column_name] == '')]
        fail_indices = fails_df.index.tolist()

    # For unique check, keep first instance and drop all other 
    elif t_check_type == 'unique_check':
        duplicates = filtered_df[filtered_df.duplicated(subset=[column_name], keep='first')]
        fail_indices = fails_df.index.tolist()


    # For master check
    elif t_check_type == 'master_check':

        master_source, master_table, master_primary_key_column, master_primary_key_value, master_value_column = master_check_details
        
        if pd.isna(master_source):
            logging.warning("Master Source not found for Master Check")
            log_error(file, check_type, error_reporting_tag, dq_column_name=column_name, error_message = "Master Source not found for Master Check", error_type=error_type)
            return
        
                
        if pd.isna(master_table) or pd.isna(master_value_column):
            logging.warning("Master Check is not configured properly")
            log_error(file, check_type, error_reporting_tag, dq_column_name=column_name, error_message = "Master Check is not configured properly", error_type=error_type)
            return

        master_source = master_source.lower().replace(" ","_")
        
        if master_source == 'config_file':
            if master_table not in all_config_masters.keys(): 
                logging.warning(f"Master table not found: {master_table}")
                log_error(file, check_type, error_reporting_tag, dq_column_name=column_name, error_message = f"Master table not found: {master_table}", error_type=error_type)
                return
            master_table_df = all_config_masters[master_table]
            logging.warning("Picked up Master Table from config file")

        elif master_source == 'source_data_table':
            if master_table not in all_source_table_masters.keys(): 
                logging.warning(f"Master table not found: {master_table}")
                log_error(file, check_type, error_reporting_tag, dq_column_name=column_name, error_message = f"Master table not found: {master_table}", error_type=error_type)
                return
            master_table_df = all_source_table_masters[master_table]
            logging.warning("Picked up Master Table from Filtered Raw Data")

        elif master_source == 'system_master':
            master_value_column = master_value_column.lower().replace(" ",'_')
            if not pd.isna(master_primary_key_column):
                master_primary_key_column = master_primary_key_column.lower().replace(" ",'_')
            if master_table not in all_system_masters.keys(): 
                logging.warning(f"Master table not found: {master_table}")
                log_error(file, check_type, error_reporting_tag, dq_column_name=column_name, error_message = f"Master table not found: {master_table}", error_type=error_type)
                return
            master_table_df = all_system_masters[master_table]
            logging.warning("Picked up Master Table from Filtered Raw Data")

        else:
            logging.warning("Unknown source for Master table")
            log_error(file, check_type, error_reporting_tag, dq_column_name=column_name, error_message = "Unknown source for Master table", error_type=error_type)
            return

        if master_value_column not in master_table_df.columns:
            logging.warning(f"Master Check: 'Master Value Column':{master_value_column} does not exist in Master Table: {master_table}")
            log_error(file, check_type, error_reporting_tag, dq_column_name=column_name, error_message = f"Master Check: 'Master Value Column':{master_value_column}  does not exist in Master Table: {master_table}", error_type=error_type)
            return

        # Get the condition from master table to check against
        
        if not pd.isna(master_primary_key_column) and not pd.isna(master_primary_key_value):
            if master_primary_key_column not in master_table_df.columns:
                logging.warning(f"Master check: 'Master Primary Key Column':{master_primary_key_column} does not exist in Master Table: {master_table}")
                log_error(file, check_type, error_reporting_tag, dq_column_name=column_name, error_message = f"Master check: 'Master Primary Key Column':{master_primary_key_column} does not exist in Master Table: {master_table}", error_type=error_type)
                return
            else:
                all_conditions = list(master_table_df.loc[master_table_df[master_primary_key_column.strip()] == master_primary_key_value, master_value_column])   
        else:
            all_conditions = master_table_df[master_value_column.strip()].unique()  
        
        if all_conditions is None:
            logging.warning(f"Values for Master Check Master Name: {master_primary_key_value} not defined in master table: {master_table}")
            logging.warning("Not Validated")
            log_error(file, check_type, error_reporting_tag, dq_column_name=column_name, error_message = f"Values for Master Check Master Name: {master_primary_key_value} not defined in master table: {master_table}", error_type=error_type)
            return
        logging.warning(f"Validating Master Check values: {all_conditions}")

        temp_all_conditions = all_conditions.copy()
        all_conditions_lower = [str(condition).strip().lower() if isinstance(condition, str) else condition for condition in temp_all_conditions]
        keep_null = False
        if None in temp_all_conditions:
            null_index = temp_all_conditions.index(None)
            temp_all_conditions.pop(null_index)
            keep_null = True
            
        non_none_types = [type(x) for x in temp_all_conditions if not pd.isna(x)]
        if not non_none_types:
            logging.warning(f"Values for Master Check Master Name: {master_primary_key_value} not defined in master table: {master_table}")
            log_error(file, check_type, error_reporting_tag, dq_column_name=column_name, error_message = f"Values for Master Check Master Name: {master_primary_key_value} not defined in master table: {master_table}", error_type=error_type)
            return

        if not pd.isna(condition_datatype):
            if condition_datatype == 'datetime':
                filtered_df[column_name] = pd.to_datetime(filtered_df[column_name])
            if condition_datatype == 'float':
                if filtered_df[column_name].dtype in ['int64', 'float64']:
                    try:
                        temp_all_conditions = list(map(float, temp_all_conditions))
                        filtered_df[column_name] = filtered_df[column_name].astype(float)
                    except ValueError:
                        if isinstance(temp_all_conditions[0], str):
                            filtered_df[column_name] = filtered_df[column_name].astype(str)

        else:
            logging.warning(f"Condition Datatype is not defined, skipping this validation")
            return

        if keep_null:
            temp_df = filtered_df.copy()
            temp_df = temp_df.dropna(subset=[column_name])
            fails_df = temp_df[~(temp_df[column_name].isin(temp_all_conditions))]
        else:    
            fails_df = filtered_df[~(filtered_df[column_name].isin(temp_all_conditions))]
            
        fail_indices = fails_df.index.tolist()

    elif (pd.isna(condition_type) and not pd.isna(condition_value)) and (not pd.isna(condition_type) and pd.isna(condition_value)) :
        logging.warning("condition_type/condition_value empty")
        log_error(file, check_type, error_reporting_tag, error_message = "condition_type/condition_value is empty", error_type=error_type)
        return

    # all value checks
    elif not pd.isna(condition_type) and not pd.isna(condition_value):

        logging.warning(f"Validating Check Value Type: {check_value_type}")
        if pd.isna(check_value_type):
            check_value_type = ""


        check_value_type = check_value_type.strip().lower().replace(" ","_")
        if check_value_type == 'global_variable':
            if condition_value.lower().replace(' ','_') == 'reporting_date':
                g_condition_value = condition_value   
                condition_value = reporting_date
            else:
                logging.warning("Global variable defined not avaiable in Global Parameter Sheet, skipping this validation")
                log_error(file, check_type, error_reporting_tag, unique_identifier=None, dq_column_name=column_name, error_message = "Global variable defined not avaiable in Global Parameter Sheet, skipping this validation", error_type=error_type)
                return
            logging.warning(f"condition_value fetched for global_variable: {g_condition_value}: {condition_value}")

        if check_value_type == 'column':
            if condition_value not in filtered_df.columns:
                logging.warning(f"Condition Column does not exist: {condition_value}, skipping this validation")
                log_error(file, check_type, error_reporting_tag, unique_identifier=None, dq_column_name=column_name, error_message = f"Condition Column does not exist: {condition_value}, skipping this validation", error_type=error_type)
                return
            
            t_condition_type = condition_type.strip().lower()
            if t_condition_type == 'equal to' or condition_type == 'contains':
                fails_df = filtered_df[filtered_df[column_name] == filtered_df[condition_value]]
            elif t_condition_type == 'not equal to' or condition_type == 'does not contains':
                fails_df = filtered_df[filtered_df[column_name] != filtered_df[condition_value]]
            elif t_condition_type == 'greater than':
                fails_df = filtered_df[filtered_df[column_name] > filtered_df[condition_value]]
            elif t_condition_type == 'greater than equal to':
                fails_df = filtered_df[filtered_df[column_name] >= filtered_df[condition_value]]
            elif t_condition_type == 'less than':
                fails_df = filtered_df[filtered_df[column_name] < filtered_df[condition_value]]
            elif t_condition_type == 'less than equal to':
                fails_df = filtered_df[filtered_df[column_name] <= filtered_df[condition_value]]
            else:
                logging.warning(f"Unknown type of condition_type: {t_condition_type}")
                log_error(file, check_type, error_reporting_tag, unique_identifier=None, dq_column_name=column_name, error_message = f"Unknown type of condition_type: {condition_type}", error_type=error_type)
                return
            
            fail_indices = fails_df.index.tolist()
            
        elif check_value_type == 'mapping_set':
            try:
                mapping_conditions = mapping_set.loc[mapping_set['mapping_set'] == condition_value, 'mapping_criteria']
            except KeyError as e:
                raise Exception(f"here:{mapping_set.columns}")
            if not mapping_conditions.empty:
                mapping_values = list(mapping_conditions)
                if condition_type == 'contains':
                    filtered_df = filtered_df[[column_name].isin(mapping_values)]
                else:
                    filtered_df = filtered_df[~(filtered_df[column_name].isin(mapping_values))]
            else:
                logging.warning(f"Mapping set defined:{condition_value} not avaiable in Mapping set table, skipping this validation")
                log_error(file, check_type, error_reporting_tag, unique_identifier=None, dq_column_name=column_name, error_message = f"Mapping set defined:{condition_value} not avaiable in Mapping set table, skipping this validation", error_type=error_type)
                return

        else:
            if pd.isna(condition_value):
                logging.warning("Condition Value is not defined for Value Check")
                log_error(file, check_type, error_reporting_tag, unique_identifier=None, dq_column_name=column_name, error_message = "Condition Value is not defined for Value Check", error_type=error_type)
                return
                
            if check_value_type == "":
                logging.warning("Condition Value Type is not defined for Value Check")
                log_error(file, check_type, error_reporting_tag, unique_identifier=None, dq_column_name=column_name, error_message = "Condition Value is not defined for Value Check", error_type=error_type)
                return
                
            if pd.isna(condition_type):
                logging.warning("Condition Type is not defined for Value Check")
                log_error(file, check_type, error_reporting_tag, unique_identifier=None, dq_column_name=column_name, error_message = "Condition Type is not defined for Value Check", error_type=error_type)
                return

            try: 
                
                if not pd.isna(condition_datatype):
                    if condition_datatype == 'datetime':
                        filtered_df[column_name] = pd.to_datetime(filtered_df[column_name])
                    if condition_datatype == 'float':
                        condition_value = float(condition_value)
                        filtered_df[column_name] = filtered_df[column_name].astype(float)
                      
                t_condition_type = condition_type.strip().lower()
                if t_condition_type == 'equal to':
                    fails_df = filtered_df[filtered_df[column_name] == condition_value]
                elif t_condition_type == 'not equal to':
                    fails_df = filtered_df[filtered_df[column_name] != condition_value]
                elif t_condition_type == 'greater than':
                    fails_df = filtered_df[filtered_df[column_name] > condition_value]
                elif t_condition_type == 'greater than equal to':
                    fails_df = filtered_df[filtered_df[column_name] >= condition_value]
                elif t_condition_type == 'less than':
                    fails_df = filtered_df[filtered_df[column_name] < condition_value]
                elif t_condition_type == 'less than equal to':
                    fails_df = filtered_df[filtered_df[column_name] <= condition_value]
                else:
                    logging.warning(f"Unknown type of condition_type: {t_condition_type}")
                    log_error(file, check_type, error_reporting_tag, unique_identifier=None, dq_column_name=column_name, error_message = f"Unknown type of condition_type: {condition_type}", error_type=error_type)
                    return
                
                fail_indices = fails_df.index.tolist()
            except TypeError as e:
                logging.warning(f"Invalid comparison: {condition_type} between {condition_value} of type: {type(condition_value)} and column:{column_name} of type:{filtered_df[column_name].dtype} {e}")
                log_error(file, check_type, error_reporting_tag, unique_identifier=None, dq_column_name=column_name, error_message = f"Invalid comparison: {condition_type} between {condition_value} of type: {type(condition_value)} and column:{column_name} of type:{filtered_df[column_name].dtype}", error_type=error_type)
                return  
        
    elif not pd.isna(condition_datatype):
        if not filtered_df[column_name].isna().all():
            if condition_datatype == 'float':
                datatype_fails_df = filtered_df[~(filtered_df[column_name].apply(is_float))]
            elif condition_datatype == 'character':
                datatype_fails_df = filtered_df[~(filtered_df[column_name].apply(is_string))]
            elif condition_datatype == 'datetime':
                datatype_fails_df = filtered_df[~(filtered_df[column_name].apply(is_datetime))]

            fail_indices = datatype_fails_df.index.tolist()
        logging.warning("Only Datatype Checked")
        
    else:   
        log_error(file, check_type, error_reporting_tag, error_message = "condition_type and condition_value are empty", error_type=error_type)
        return

    # if fails log the error in report
    if not fails_df.empty:
        for i, row in fails_df.iterrows():
            unique_identifier_value = row[unique_identifier_column]
            actual_condition_value = row[column_name]
            if t_check_type == "null_check":
                log_error(file, check_type, error_reporting_tag, unique_identifier_value, column_name, f"{check_type} failed", error_type)
            elif t_check_type == "master_check":
                log_error(file, check_type, error_reporting_tag, unique_identifier_value, column_name, f"{check_type} failed: Master Table: {master_table} Master Name:{master_primary_key_value} actual:{actual_condition_value} condition_value:{all_conditions}", error_type)
            else:    
                if check_value_type == 'column':
                    expected_condition_value = row[condition_value]
                    log_error(file, check_type, error_reporting_tag, unique_identifier_value, column_name, f"{check_type} failed: condition:{condition_type} actual:{actual_condition_value} condition_value:{expected_condition_value}", error_type)                
                else:
                    log_error(file, check_type, error_reporting_tag, unique_identifier_value, column_name, f"{check_type} failed: condition:{condition_type} actual:{actual_condition_value} condition_value:{condition_value}", error_type)

    # if duplicates found, log in report
    if not duplicates.empty:
        for i,row in duplicates.iterrows():
            unique_identifier_value = row[unique_identifier_column]
            actual_condition_value = row[column_name]
            log_error(file, check_type, error_reporting_tag, unique_identifier_value, column_name, f"{check_type} failed", error_type)

    # if datatype check failed
    if not datatype_fails_df.empty:
        for i, row in datatype_fails_df.iterrows():
            unique_identifier_value = row[unique_identifier_column]
            log_error(file, check_type, error_reporting_tag, unique_identifier_value, column_name, f"Datatype check failed: {condition_datatype}", error_type)
    
    logging.warning("Validated")

    if fail_indices:
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
    
    logging.warning("   ")
    
    # Iterate over each table
    for file, validations in all_validations:
        
        file_path = os.path.join(parquet_raw_path,f"{file}.parquet")

        # if file != 'indus_pos_data':
        #     continue

        # File read
        logging.warning(f"Reading {file}")
                        
        if os.path.exists(file_path):
            df = pd.read_parquet(file_path)
        else:
            logging.warning(f"Source data file for '{file}' not found. Skipping this source.")
            continue
        
        logging.warning("File read successfully")
        logging.warning("   ")

        logging.warning(f"No of rows to validate {len(validations)} in {file}")

        indices_to_drop = set() # For FATAL error

        # Iterate over each validation in a each table in each system_name
        for i,validation in validations.iterrows():
            logging.warning("   ")
            logging.warning(f"Validating row:{i+2}")
    
            # if i+2 != 10: # add the row value for which you have to test
            #     continue
        
            # pre filter values
            filter_column = validation['filter_column']
            filter_condition_datatype = validation['filter_condition_datatype']
            filter_condition_type = validation['filter_condition_type']
            filter_condition_value = validation['filter_condition_value']
            filter_value_type = validation['filter_value_type']

            # actual check values
            column_name = validation['source_column_name']
            check_condition_datatype = validation['check_condition_datatype']
            check_condition_type = validation['check_condition_type']
            check_condition_value = validation['check_condition_value']
            check_value_type = validation['check_value_type']

            # for master check
            master_source = validation["master_source"]	
            master_table = validation["master_table"]	
            master_value_column = validation["master_value_column"]	
            master_primary_key_column = validation["master_primary_key_column"]	
            master_primary_key_value = validation["master_primary_key_value"]
            
            # other values
            unique_identifier_column = validation['unique_identifier']
            error_reporting_tag = validation['error_reporting_tag']
            check_type = validation['check_type']
            error_type = validation['error_type']

            if pd.isna(column_name):
                log_error(file, check_type, error_reporting_tag_value=None, unique_identifier=None, dq_column_name=column_name, error_message="Source Column Name is empty, skipping this validation", error_type=error_type)
                logging.warning("Source Column Name is empty, skipping this validation")
                continue
            else:
                column_name = column_name.strip()
            
            if pd.isna(unique_identifier_column):
                log_error(file, check_type, error_reporting_tag_value=None, unique_identifier=None, dq_column_name=column_name, error_message="Unique Identifier Column is empty, skipping this validation", error_type=error_type)
                logging.warning("Unique Identifier Column is empty, skipping this validation")
                continue
            else:
                unique_identifier_column = unique_identifier_column.strip()
            
            if pd.isna(check_condition_datatype):
                log_error(file, check_type, error_reporting_tag_value=None, unique_identifier=None, dq_column_name=column_name, error_message="Check Condition Datatype is empty, skipping this validation", error_type=error_type)
                logging.warning("Check Condition Datatype is empty, skipping this validation")
                continue
            else:
                check_condition_datatype = check_condition_datatype.lower().strip()
            
            if pd.isna(check_type):
                log_error(file, check_type, error_reporting_tag_value=None, unique_identifier=None, dq_column_name=column_name, error_message="Check Type is empty, skipping this validation", error_type=error_type)
                logging.warning("Check Type is empty, skipping this validation")
                continue
            else:
                check_type = check_type.strip()

            if pd.isna(error_type):
                log_error(file, check_type, error_reporting_tag_value=None, unique_identifier=None, dq_column_name=column_name, error_message="Error Type is empty, skipping this validation", error_type=error_type)
                logging.warning("Error Type is empty, skipping this validation")
                continue
            else:
                error_type = error_type.strip()    
            
            if not pd.isna(error_reporting_tag):
                error_reporting_tag = error_reporting_tag.strip()
            
            filter_details = [filter_column, filter_condition_datatype, filter_condition_type, filter_condition_value, filter_value_type]
            check_details = [column_name, check_condition_datatype, check_condition_type, check_condition_value, check_value_type]
            master_check_details = [master_source, master_table, master_primary_key_column, master_primary_key_value, master_value_column]
            
            df.columns = df.columns.str.strip()            
            
            if column_name not in df.columns:
                log_error(file, check_type, error_reporting_tag_value=None, unique_identifier=None, dq_column_name=column_name, error_message=f"Column does not exist: {column_name}", error_type=error_type)
                logging.warning(f"Column does not exist: {column_name}")
                continue
            try:
              validate_data(file, df, filter_details, check_details, master_check_details, check_type, unique_identifier_column, error_reporting_tag, error_type, indices_to_drop)
            except Exception as e:
              raise Exception(f"{i+2}: {e}")
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
    all_checks.columns = all_checks.columns.str.replace(" ","_").str.lower()
    # raise Exception(all_checks.columns)
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
        
        # Create directories if not exist
        try:
            shutil.rmtree(parquet_error_free_path)
            os.makedirs(parquet_error_free_path)   
        except FileNotFoundError:
            os.makedirs(parquet_error_free_path)    
        except OSError as e:
            logging.warning(f"Error deleting Raw Data directory: {e}")

        for table_name, temp_df in all_validated_df.items():
            try:
                # save to Raw Data Directory
                df_save_path_parquet = os.path.join(parquet_error_free_path,f"{table_name}.parquet")
                temp_table = pa.Table.from_pandas(temp_df)
                pq.write_table(temp_table, df_save_path_parquet)

                # output_file = r"C:\Users\BAnilKumar\Downloads\dq\Error Free Data 2"
                
                # output_file = os.path.join(output_file,f"{table_name}.xlsx")
                # temp_df.to_excel(output_file)

                # save to local for testing
                output_file = fr"/home/azureuser/DataFrameWork Testing/Latest/Error Free Data/{table_name}.csv"
                excel_buffer = BytesIO()
                temp_df.to_csv(excel_buffer, index=False)
                excel_buffer.seek(0)
                sftp_client.putfo(excel_buffer, output_file)
                logging.warning(f"Parquet saved for {table_name}, ")
            except Exception as e:
                logging.warning(f"Error in saving parquet for {table_name}, {e}")

        try:
            ssh_client.close()
            sftp_client.close()
        except Exception as e:
            logging.warning(f"{e}")
    else:
        logging.warning("No data to be saved.")


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
        ssh_client.connect(hostname=hostname, username=username, password=user_secret_key, port=port, banner_timeout=200, look_for_keys=False)
        sftp_client = ssh_client.open_sftp()
        logging.warning("Connected to the SFTP server.")
        return sftp_client, ssh_client
    
    except ssh_exception.AuthenticationException as e:
        logging.warning(f"AuthenticationException Connnecting to the sftp: {Sftp_Name}: {e}")
        return None, None

    except Exception as e:
        logging.warning(f"Error Connnecting to the sftp: {Sftp_Name}: {e}")
        return None, None


def read_all_master(source_data_validation):
    """
    Read Masters for Master check
    """

    source_data_table_masters_list = source_data_validation.loc[source_data_validation['master_source'].str.lower().str.replace(" ","_") == 'source_data_table', 'master_table']
    source_data_table_masters_list = source_data_table_masters_list.unique()
    
    all_source_table_master = {}
    for master in source_data_table_masters_list:
        if not pd.isna(master):
            # Reading source table for master check
            logging.warning(f"Reading master table: {master}")
            file_path = os.path.join(parquet_raw_path,f"{master}.parquet")
                       
            if os.path.exists(file_path):
                all_source_table_master[master] = pd.read_parquet(file_path)
                logging.warning(f"Read Master for master check: {master}")
            else:
                logging.warning(f"Source data file for '{master}' not found. Skipping this master.")
                raise Exception(f"Source data file for '{master}' not found. Skipping this master.")
        

    return all_source_table_master



# Main Code

global parquet_path, parquet_raw_path, parquet_error_free_path
parquet_path = os.path.join(PLATFORM_FILE_PATH, "alm_data/Data Framework")
parquet_raw_path = os.path.join(parquet_path, "Raw Data")
parquet_error_free_path = os.path.join(parquet_path, "Error Free Data")

if not os.path.exists(parquet_raw_path):
    raise Exception("No Raw Data found")

try:
    sftp_client, ssh_client = get_sftp_connection('Antares SFTP')
except Exception as e:
    logging.warning(f"{e}")

# Validation, Master Repository 
global source_data_validation, mapping_set
source_data_validation = Data6
mapping_set = Data5

logging.warning(f"All config sheets are loaded")

# Read all master tables for master check
global all_source_table_masters, all_system_masters
all_source_table_masters = read_all_master(source_data_validation)
all_system_masters = {}
all_system_masters['master_repository'] = Data4
# all_system_masters['client_product_master'] = Data1
# all_system_masters['counterparty_master'] = Data2
# all_system_masters['gl_master'] = Data3

logging.warning("Read all Master table for master checks:")
logging.warning(f"Master source source_data_table: {all_source_table_masters.keys()}")
logging.warning(f"Master source system_masters:{all_system_masters.keys()}")

# Read extract_date and dq_run_date to log in DQ report
global extract_date, reporting_date
extract_date = Reporting_Date
reporting_date = Reporting_Date

global dq_run_date
dq_run_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M')

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

# validation_error_df.to_csv(r"C:\Users\BAnilKumar\Downloads\dq\DQ-18-JAN.csv")
output_data = validation_error_df.astype(str)

logging.warning("   ")
logging.warning("Validation Completed")
