# -*- coding: utf-8 -*-
"""
Created on Wed Jan 15 21:19:29 2025

@author: BAnilKumar
"""

import numpy as np
import pandas as pd
import logging
import os
global pa, pq
# import pyarrow as pa
# import pyarrow.parquet as pq
global paramiko, ssh_exception, BytesIO
from io import BytesIO
import paramiko
from paramiko import ssh_exception

global read_configurations
def read_configurations(config_file):
    """
    Reads the configuration Excel file and returns the necessary DataFrames.
    """
    try:
        
        
        logging.warning("Reading configuration file.")
        transformation_mapping = pd.read_excel(config_file, sheet_name='transformation_mapping')
        transformation_mapping.columns = transformation_mapping.columns.str.replace(" ","_").str.lower()
        logging.warning("Read transformation mapping.")
        
        master_repository = pd.read_excel(config_file, sheet_name='master_repository')
        master_repository.columns = master_repository.columns.str.replace(" ","_").str.lower()
        logging.warning("Read master repository.")
        
        conditional_rules = pd.read_excel(config_file, sheet_name='conditional_rule_definition')
        conditional_rules.columns = conditional_rules.columns.str.replace(" ","_").str.lower()
        logging.warning("Read conditional rule definition.")
        
        rule_group_definition = pd.read_excel(config_file, sheet_name='rule_group_definition')
        rule_group_definition.columns = rule_group_definition.columns.str.replace(" ","_").str.lower()
        logging.warning("Read rule group definition.")
        
        master_mapping_rule_definition = pd.read_excel(config_file, sheet_name='master_mapping_rule_definition')
        master_mapping_rule_definition.columns = master_mapping_rule_definition.columns.str.replace(" ","_").str.lower()
        logging.warning("Read master mapping rule definition.")
        
        global_parameter = pd.read_excel(config_file, sheet_name='global_parameter')
        global_parameter.columns = global_parameter.columns.str.replace(" ","_").str.lower()
        logging.warning("Read global parameter")
        
        logging.warning("Configuration file read successfully.")
        logging.warning(f"  ")  #for testing
        logging.warning(f"  ")  #for testing
        logging.warning(f"  ")  #for testing
        return transformation_mapping, conditional_rules, master_repository, rule_group_definition, master_mapping_rule_definition, global_parameter
        
    except Exception as e:
        logging.warning(f"Error reading configuration file: {e}")
        # raise Exception(f"Error reading configuration file: {e}")
        
        return None, None, None, None, None  # Return None if error occurs

global apply_thresholds
def apply_thresholds(values_to_assign, rule_group_row):
    """
    Apply threshold and return values
    """
    try:
        logging.warning("Inside apply_thresholds")
        threshold_source_table = rule_group_row.iloc[0]['threshold_source_table']
        threshold_filter_column = rule_group_row.iloc[0]['threshold_filter_column']
        threshold_filter_value = rule_group_row.iloc[0]['threshold_filter_value']
        threshold_value = rule_group_row.iloc[0]['threshold_value']
        operation_parameter = rule_group_row.iloc[0]['operation_parameter']
        
        # skip applying threshold if not any of the threshold arguments are empty
        if pd.isna(threshold_source_table) or pd.isna(threshold_filter_column) or pd.isna(threshold_filter_value) or pd.isna(threshold_value) or pd.isna(operation_parameter):
            logging.warning("Threshold parameters are not set, returning original values")
            return values_to_assign
        
        # Apply threshold
        # Get the source table from source data
        if threshold_source_table not in source_data:
            logging.warning(f"Source data for '{threshold_source_table}' not available, returning original values.")
            return

        data = source_data[threshold_source_table]
        cond = data[threshold_filter_column] == threshold_filter_value

        # changes in the values
        values_to_assign = values_to_assign.where(cond, values_to_assign * threshold_value)

        return values_to_assign

    except Exception as e:
        logging.warning(f"Error in Applying Thresholds: {e}")

global apply_value_adjustment
def apply_value_adjustment(values, operation, operation_value, source_data_type):
    """
    Applies value adjustment operation to a pandas Series.
    If either operation or operation_value is None, returns the input values without transformation.
    """
    logging.warning(f"Applying Value Adjsutment")
    logging.warning(f"operation {operation} ")
    logging.warning(f"operation_value {operation_value} ")
    logging.warning(f"source_data_type {source_data_type} ")
    # logging.warning(f"values {values} ")


    if pd.isna(operation) or pd.isna(operation_value):
        logging.warning("Operation or operation_value is None or NaN. Returning original values.")
        logging.warning(f"  ")  #for testing
        return values
    
    if operation is None or operation_value is None:
        logging.warning("Operation or operation_value is None. Returning input values without transformation.")
        logging.warning(f"  ")  #for testing
        return values

    if pd.isna(source_data_type):
        logging.warning("Source data is None. Returning original values.")
        logging.warning(f"  ")  #for testing
        return values

    logging.warning("Applying datatype")
    try:
        if source_data_type.upper() == 'DATETIME':
            values = pd.to_datetime(values, format='%Y-%m-%d', errors='coerce')
            operation_value = pd.to_datetime(operation_value, format='%Y-%m-%d', errors='coerce')
        elif source_data_type.upper() == 'FLOAT':
            values = pd.to_numeric(values, errors='coerce')
            operation_value = pd.to_numeric(operation_value, errors='coerce')
        elif source_data_type.upper() == 'STRING':
            values = values.astype(str)
            operation_value = str(operation_value)
        else:
            logging.warning(f"  ")  #for testing
            logging.warning(f"Unknown source data type: {source_data_type}")
            logging.warning(f"  ")  #for testing
            return values
    except Exception as e:
        logging.warning(f"Error applying source data type: {e}")
        return values

    logging.warning("Applying operation")
    try:
        if operation.upper() == 'ADD':
            values = values + operation_value
        elif operation.upper() == 'SUBTRACT':
            values = values - operation_value
        elif operation.upper() == 'MULTIPLY':
            values = values * operation_value
        elif operation.upper() == 'DIVIDE':
            if operation_value != 0:
                values = values / operation_value
            else:
                logging.warning("Division by zero or None encountered. Returning original values")
    except Exception as e:
        logging.warning(f"Error applying value adjustment operation:{operation} operation_value:{operation_value} :{e}")
        return values
    
    return values

global process_conditional_operation
def process_conditional_operation(row, source_data, conditional_rules, target_data, rule_group_definition):
    """
    Processes conditional operations for a given row.
    """
    logging.warning(f"Inside process_conditional_operation function.")
    try:
        target_table_name = row['target_table_name']
        target_column_name = row['target_table_column']
        source_table_name = row['source_table_name']
        transformation_rule_reference = row['transformation_rule_reference']
        value_adjustment_operation = row.get('value_adjustment_operation', None)
        transformation_operation_value = row.get('transformation_operation_value', None)
        source_data_type = row.get('source_data_type')

        logging.warning(f"Processing conditional operation for target column '{target_column_name}' using transformation rule reference '{transformation_rule_reference}'.")

        # Get the rule_set from rule_group_definition
        rule_group_rows = rule_group_definition[rule_group_definition['rule_group'] == transformation_rule_reference]
        if rule_group_rows.empty:
            logging.warning(f"No rule set found for rule group '{transformation_rule_reference}'.")
            return
        
        final_target_series = None

        for idx, rule_group_row in rule_group_rows.iterrows():
            rule_group_operation = rule_group_row['rule_group_operation'] 

            # Perform Rule group operation, if multiple rule sets under rule_group
            if len(rule_group_rows) > 1:
                if pd.isna(rule_group_operation):
                    logging.warning(f"Multiple Rule Sets under Rule_group: {transformation_rule_reference} but rule_group_operation is not defined. Skipping this transformation")
                    return
            
            rule_set = rule_group_row['rule_set'] 
            operation = rule_group_operation.strip().upper()

            logging.warning(f"Found rule set '{rule_set}' for rule group '{transformation_rule_reference}'.")

            # Get the conditional rules from conditional_rules where Rule_ref == rule_set
            relevant_rules = conditional_rules[conditional_rules['rule_ref'] == rule_set]
            if relevant_rules.empty:
                logging.warning(f"No conditional rules found for rule set '{rule_set}'.")
                return

            # Get the data from source table
            if source_table_name not in source_data:
                # logging.warning(f"Source data for '{source_table_name}' not available. Skipping this mapping.")
                return

            data = source_data[source_table_name]

            # Prepare the target column Series
            target_series = pd.Series(index=data.index, dtype=object)

            # For each condition, process and set the values
            for idx, cond_row in relevant_rules.iterrows():
                rule_set = cond_row['rule_set']
                condition_operation = cond_row['condition_operation']
                condition_source_table = cond_row['condition_source_table']
                condition_column_name = cond_row['condition_column_name']
                condition_datatype = cond_row['condition_datatype']
                condition_type = cond_row['condition_type']
                condition_value = cond_row['condition_value']
                target_column_value = cond_row['target_column_value']
                target_column_value_source = cond_row['target_column_value_source']

                logging.warning(f"In rule_set: {rule_set}")

                # for direct mapping, return column as is
                if condition_operation is True:
                    target_value_data = target_data[target_column_value_source]

                    if target_column_value not in target_value_data.columns:
                        # logging.warning(target_value_data.columns)
                        logging.warning(f"Target column '{target_column_value}' not found in '{target_column_value_source}'. Skipping this condition.")
                        continue
                    
                    target_series = target_value_data[target_column_value]
                    logging.warning("Condition true, returning original values from target data")
                    
                else:

                    # Get the condition_source_table data
                    if condition_source_table not in source_data:
                        logging.warning(f"Condition source data for '{condition_source_table}' not available. Skipping this condition.")
                        continue

                    cond_data = source_data[condition_source_table]

                    # Evaluate the condition
                    if condition_column_name not in cond_data.columns:
                        logging.warning(f"Condition column '{condition_column_name}' not found in '{condition_source_table}'. Skipping this condition.")
                        continue

                    # condition type is not defined, then continue 
                    if pd.isna(condition_type):
                        logging.warning(f"Condition Type is not defined. Skipping this condition.")
                        continue

                    # Get the column to evaluate
                    condition_column = cond_data[condition_column_name]

                    # Apply value adjustment to condition_column if needed
                    # For now, we proceed without data type conversion for the condition column

                    condition_type = condition_type.strip().upper()
                    logging.warning(f"condition_type {condition_type} ")
                    if condition_type == 'EMPTY':
                        cond = condition_column.isnull() | (condition_column == '')
                    elif condition_type == 'NOT EMPTY':
                        cond = ~(condition_column.isnull() | (condition_column == ''))
                    elif condition_type == 'EQUAL':
                        cond = condition_column == condition_value
                        logging.warning(f"after applying condition: {cond}")
                    elif condition_type == 'NOT EQUAL TO':
                        cond = condition_column != condition_value
                    else:
                        logging.warning(f"Unknown condition type '{condition_type}'. Skipping this condition.")
                        continue

                    # Get the values to assign from target_column_value_source and target_column_value
                    if target_column_value_source not in source_data:
                        logging.warning(f"Target column value source data '{target_column_value_source}' not available. Skipping this condition.")
                        continue

                    target_value_data = source_data[target_column_value_source]

                    if target_column_value not in target_value_data.columns:
                        logging.warning(f"Target column '{target_column_value}' not found in '{target_column_value_source}'. Skipping this condition.")
                        continue

                    # Get the values to assign
                    values_to_assign = target_value_data[target_column_value]
                    # logging.warning(f"values_to_assign {values_to_assign} ")

                    # Apply value adjustment if specified
                    if value_adjustment_operation and transformation_operation_value:
                        values_to_assign = apply_value_adjustment(values_to_assign, value_adjustment_operation, transformation_operation_value, source_data_type)

                    # Assign the values where the condition is True
                    target_series.loc[cond] = values_to_assign.loc[cond]

            # Apply threshold if specified
            target_series = apply_thresholds(target_series, rule_group_rows)

            logging.warning(f"Performing rule_group_operation: {operation}")
            try:
                if operation == 'ADD':
                    if final_target_series is None:
                        final_target_series = target_series
                    else:
                        final_target_series += target_series

                elif operation == 'SUBTRACT':
                    if final_target_series is None:
                        final_target_series = target_series
                    else:
                        final_target_series -= target_series

                elif operation == 'MULTIPLY':
                    if final_target_series is None:
                        final_target_series = target_series
                    else:
                        final_target_series *= target_series

                elif operation == 'DIVIDE':
                    if final_target_series is None:
                        final_target_series = target_series
                    else:
                        final_target_series = np.where(target_series != 0, final_target_series / target_series, final_target_series)
                        logging.warning("Division by zero or None is be skipped")
            except Exception as e:
                logging.warning(f"Error in rule_group_opration: {e}")
                return
        
            
        if source_table_name not in target_data:
            target_data[source_table_name] = pd.DataFrame(index=data.index)


        # Assign the target_series to the target column
        # logging.warning(f"final_target_series {final_target_series} ")
        target_data[source_table_name][target_column_name] = final_target_series

        logging.warning(f"Conditional operation completed for target column '{target_column_name}'.")

    except Exception as e:
        logging.warning(f"Error in conditional operation: {e}")
        # Error handled gracefully without raising an exception

global process_direct_mapping
def process_direct_mapping(row, source_data, target_data):
    """
    Processes direct mapping for a given row.
    """
    try:
        target_table_name = row['target_table_name']
        target_column_name = row['target_table_column']
        source_column_name = row['source_column_name']
        source_table_name = row['source_table_name']
        value_adjustment_operation = row.get('value_adjustment_operation', None)
        transformation_operation_value = row.get('transformation_operation_value', None)
        transformation_rule_reference = row['transformation_rule_reference']
        source_data_type = row.get('source_data_type')

        logging.warning(f"Processing direct mapping for target column '{target_column_name}' from source column '{source_column_name}'.")

        if source_table_name not in source_data:
            # logging.warning(f"Source data for '{source_table_name}' not available. Skipping this mapping.")
            return

        data = source_data[source_table_name]
        if source_table_name not in target_data:
            target_data[source_table_name] = pd.DataFrame(index=data.index)

        values_to_assign = data[source_column_name]

        # Apply value adjustment if specified
        if value_adjustment_operation and transformation_operation_value:
            values_to_assign = apply_value_adjustment(values_to_assign, value_adjustment_operation, transformation_operation_value, source_data_type)

        # Get the rule_group_row from rule_group_definition and apply threshold if specified
        rule_group_row = rule_group_definition[rule_group_definition['rule_group'] == transformation_rule_reference]
        if not rule_group_row.empty:
            values_to_assign = apply_thresholds(values_to_assign, rule_group_row)
            logging.warning(f"No rule set found for rule group '{transformation_rule_reference}'.")
            return

        target_data[source_table_name][target_column_name] = values_to_assign

    except Exception as e:
        logging.warning(f"Error in direct mapping: {e}")
        # raise Exception(f"Error in direct mapping: {e}")
        # Error handled gracefully without raising an exception

global process_master_mapping
def process_master_mapping(row, source_data, master_repository, target_data, rule_group_definition, master_mapping_rule_definition):
    """
    Processes master mapping operation for a given row, considering additional mapping layers.
    """
    try:
        target_table_name = row['target_table_name']
        target_column_name = row['target_table_column']
        source_column_name = row['source_column_name']
        source_table_name = row['source_table_name']
        transformation_rule_reference = row['transformation_rule_reference']
        logging.warning(f"Processing master mapping for target column '{target_column_name}' using transformation rule reference '{transformation_rule_reference}'.")

        if source_table_name not in source_data:
            # logging.warning(f"Source data for '{source_table_name}' not available. Skipping this mapping.")
            return

        data = source_data[source_table_name]

        # Step 1: Get the rule_set from rule_group_definition using transformation_rule_reference
        rule_group_row = rule_group_definition[rule_group_definition['rule_group'] == transformation_rule_reference]
        if rule_group_row.empty:
            logging.warning(f"No rule set found for rule group '{transformation_rule_reference}'.")
            return
        rule_set = rule_group_row.iloc[0]['rule_set']
        logging.warning(f"Found rule set '{rule_set}' for rule group '{transformation_rule_reference}'.")

        # Step 2: Get the master mapping details from master_mapping_rule_definition using rule_set
        master_mapping_row = master_mapping_rule_definition[master_mapping_rule_definition['rule_ref'] == rule_set]
        if master_mapping_row.empty:
            logging.warning(f"No master mapping found for rule set '{rule_set}'.")
            return
        # Assuming only one row per rule_set
        master_mapping_row = master_mapping_row.iloc[0]
        master_table_name = master_mapping_row['master_table_name']
        master_source_column = master_mapping_row['master_source_column']
        master_target_column = master_mapping_row['master_target_column']
        logging.warning(f"Master mapping details: master_table_name='{master_table_name}', master_source_column='{master_source_column}', master_target_column='{master_target_column}'.")

        # Step 3: Get the actual mappings from master_repository using master_table_name
        master_data = master_repository[master_repository['master_name'] == master_table_name]
        if master_data.empty:
            logging.warning(f"No master data found for master table '{master_table_name}'.")
            return

        mapping_dict = dict(zip(master_data[master_source_column], master_data[master_target_column]))
        logging.warning(f"Mapping dictionary created with {len(mapping_dict)} entries.")

        if source_table_name not in target_data:
            target_data[source_table_name] = pd.DataFrame(index=data.index)

        # Apply the mapping to the source column
        source_values = data[source_column_name].map(mapping_dict)

        # Apply threshold if specified
        source_values = apply_thresholds(source_values, rule_group_row)

        target_data[source_table_name][target_column_name] = source_values
    except Exception as e:
        logging.warning(f"Error in master mapping: {e}")
        # raise Exception(f"Error in master mapping: {e}")
        # Error handled gracefully without raising an exception

global process_global_operation
def process_global_operation(row, global_parameter, source_data, target_data):
    """
    Processes global parameter mapping for a given row.
    """
    try:
        target_table_name = row['target_table_name']
        target_column_name = row['target_table_column']
        source_table_name = row.get('source_table_name', None)
        
        logging.warning(f"Processing global parameter mapping for target column '{target_column_name}' in table '{target_table_name}'.")

        # Get the value from global_parameter DataFrame
        # Filter global_parameter where 'column_name' == target_column_name
        matching_param = global_parameter[global_parameter['column_name'] == target_column_name]
        if matching_param.empty:
            logging.warning(f"No matching global parameter found for column '{target_column_name}'. Skipping.")
            return

        value = matching_param.iloc[0]['value']

        # Now, assign this value to the target_data
        # Determine the number of rows

        if source_table_name in target_data:
            # Use existing target_data[target_table_name]
            num_rows = len(target_data[source_table_name])
            index = target_data[source_table_name].index
        elif target_table_name and source_table_name in source_data:
            # Use the index of the source_data[source_table_name]
            num_rows = len(source_data[source_table_name])
            index = source_data[source_table_name].index
            # Initialize the target_data[target_table_name]
            target_data[target_table_name] = pd.DataFrame(index=index)
        else:
            # Cannot determine the number of rows
            logging.warning(f"Cannot determine the number of rows for target table '{target_table_name}'. Skipping.")
            return

        # Create a Series with the constant value
        values_to_assign = pd.Series([value] * num_rows, index=index)

        # Assign the value to the target column
        target_data[source_table_name][target_column_name] = values_to_assign.values

        logging.warning(f"Assigned global parameter value '{value}' to column '{target_column_name}' in table '{target_table_name}'.")

    except Exception as e:
        logging.warning(f"Error in global parameter mapping: {e}")
        # Error handled gracefully without raising an exception

global process_transformations
def process_transformations(transformation_mapping, conditional_rules, master_repository, source_data, target_data, rule_group_definition, master_mapping_rule_definition):
    """
    Processes all the transformations as per the transformation mapping.
    """
    try:
        # Get unique source_table_names
        unique_source_tables = transformation_mapping['source_table_name'].unique()
        # Variable to control outer loop
        break_outer = False

        for source_table in unique_source_tables:
            if source_table != 'pennant_pos_data':
                continue

            logging.warning(f" ")
            logging.warning(f"Processing transformations for source table '{source_table}'.")
            # Break the outer loop if needed
            # if source_table == 'Your_Break_Source_Table_Name':
            #     break

            # Filter the transformation mapping for this source_table
            transformations_for_source = transformation_mapping[transformation_mapping['source_table_name'] == source_table]

            # If source_table not in source_data, skip
            if source_table not in source_data:
                logging.warning(f"Source data for '{source_table}' not available. Skipping transformations for this source.")
                logging.warning(f"   ")
                logging.warning(f"   ")
                continue

            # Variable to control inner loop
            # breaking_at_index = 10  # Set to desired index to break inner loop
            # current_idx = 0
            

            # Process each transformation for this source_table
            for idx, row in transformations_for_source.iterrows():
                # if idx+2 < 176 or idx+2 > 178:
                # if idx+2 != 167:
                #     continue
                transformation_type = row['transformation_type']
                logging.warning(f"Prcoessing row:{idx+2}")
                
                # Break the inner loop if needed
                # if current_idx >= breaking_at_index:
                #     break
                # current_idx += 1

                if transformation_type == 'Direct Mapping':
                    # continue
                    process_direct_mapping(row, source_data, target_data)
                elif transformation_type == 'Master Mapping Operations':
                    # continue 
                    process_master_mapping(row, source_data, master_repository, target_data, rule_group_definition, master_mapping_rule_definition)
                elif transformation_type == 'Conditional Operations':
                    # continue
                    process_conditional_operation(row, source_data, conditional_rules, target_data, rule_group_definition)
                elif transformation_type == 'Global Parameter':
                    # continue
                    process_global_operation(row, global_parameter, source_data, target_data)
                else:
                    # continue
                    logging.warning(f"source table name { source_table} Unknown transformation type '{transformation_type}'. Skipping row {idx}.")

            # Uncomment to break the outer loop after processing the first source_table
            # break
            
            import time
            time.sleep(5)   #enoughf time to read log properly.
            
    except Exception as e:
        logging.warning(f"Error processing transformations: {e}")
        # raise Exception(f"Error processing transformations: {e}")
        # Error handled gracefully without raising an exception
        
global read_source_data
def read_source_data(source_table_names):
    """
    Reads source data from individual files, supporting Excel and CSV formats.
    """
    source_data = {}
    
    for source_table_name in source_table_names:
        try:
            logging.warning(f"Reading source data for '{source_table_name}'.")
            
            # read from parquet
            data = pd.read_csv(fr"C:\Users\BAnilKumar\Downloads\dq\AFL ETL 21 JAN 5 PM\Error Free Data\{source_table_name}.csv")
            source_data[source_table_name] = data
            
        except Exception as e:
            logging.warning(f"Error reading source data for '{source_table_name}': {e}")
            # Error handled gracefully without raising an exception
    return source_data

global write_output_data
def write_output_data(target_data):
    """
    Writes the target tables to separate Excel files.
    """
    
    try:
        logging.warning("Writing output data to target Excel files.")
        
        for table_name, data in target_data.items():
            # df_save_path_parquet = os.path.join(parquet_transformed_path, f"{table_name}.parquet")
            # logging.warning(f"  ")  #for testing
            logging.warning(f"Writing data for target table '{table_name}' to .")
            
            # write df to parquet
            # temp_table = pa.Table.from_pandas(data)
            # pq.write_table(temp_table, df_save_path_parquet)


            output_file = r"C:\Users\BAnilKumar\Downloads\dq\Transformed Data 3"
            
            output_file = os.path.join(output_file,f"{table_name}.csv")
            data.to_csv(output_file)
            # df_save_path = fr"/home/azureuser/DataFrameWork Testing/Transformed Data/{table_name}.xlsx"
            # excel_buffer = BytesIO()
            # data.to_excel(excel_buffer, index=False)
            # excel_buffer.seek(0) 
            # sftp_client.putfo(excel_buffer, df_save_path)

        logging.warning("Output data written successfully.")
    except Exception as e:
        logging.warning(f"Error writing output data: {e}")
        # raise Exception(f"Error writing output data: {e}")
        # Error handled gracefully without raising an exception

# Read configurations
global config_file_path
config_file_path = "Config_File_Path"

# sftp_creds = Data1

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
    ssh_client.connect(hostname=hostname, username=username, password=user_secret_key, port=port, banner_timeout=200, look_for_keys=False)
    sftp_client = ssh_client.open_sftp()
    logging.warning("Connected to the SFTP server.")

except ssh_exception.AuthenticationException as e:
    logging.warning(f"AuthenticationException Connnecting to the sftp: {username}: {e}")

except Exception as e:
    logging.warning(f"Error Connnecting to the sftp: : {e}")

# Read Antares Master File
try:
    with sftp_client.file(config_file_path, 'rb') as remote_file:
        file_data = remote_file.read()
    master_file_buffer = BytesIO(file_data)
except Exception:
    logging.warning(f"File not found:{config_file_path}")

global parquet_path, parquet_error_free_path, parquet_transformed_path
parquet_path = r"/opt/revolutio/Platform_Configs/alm_data/Data Framework"
parquet_error_free_path = os.path.join(parquet_path, "Error Free Data")
parquet_transformed_path = os.path.join(parquet_path, "Transformed Data")

# Create directories if not exist
if not os.path.exists(parquet_error_free_path):
    raise Exception("No Error Free Data found to transform")

os.makedirs(parquet_transformed_path, exist_ok=True)

config_file = pd.ExcelFile(r"C:\Users\BAnilKumar\Downloads\Antares Data Framework - 9th Jan 2025.xlsx")

try:
    # transformation_mapping, conditional_rules, master_repository, rule_group_definition, master_mapping_rule_definition = read_configurations(config_file)
    transformation_mapping, conditional_rules, master_repository, rule_group_definition, master_mapping_rule_definition, global_parameter = read_configurations(config_file)
except ValueError as e:
    logging.warning("Global parameter Sheet not found")
except Exception as e:
    raise Exception(f"Error in reading config data: {e}")

# Check if configurations were read successfully
if transformation_mapping is None:
    logging.warning("Failed to read configurations. Exiting program.")
else:
    # Get unique source table names
    source_table_names = transformation_mapping['source_table_name'].unique()

    # Read source data
    source_data = read_source_data(source_table_names)
  
    # Create empty target_data dictionary
    global target_data
    target_data = {}

    # Process transformations
    process_transformations(transformation_mapping, conditional_rules, master_repository, source_data, target_data, rule_group_definition, master_mapping_rule_definition)

    # Write output data
    write_output_data(target_data)

    file_names = [os.path.splitext(i)[0] for i in os.listdir(parquet_error_free_path)]
    file_status = [file_name in target_data.keys() for file_name in file_names]
    output_data = pd.DataFrame({
        "Table Name": file_names,
        "Transformation Status": file_status
    })
    output_data["id"] = output_data.index + 1
    output_data.insert(0, 'id', output_data.pop('id'))

