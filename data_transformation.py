global BytesIO, shutil, paramiko, ssh_exception
global PLATFORM_FILE_PATH, decrypt_existing_sftp_credentials
global pa, pq
import os
import logging
import json
import shutil
from io import BytesIO
import paramiko
from paramiko import ssh_exception
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from config.settings.base import PLATFORM_FILE_PATH
from Core.users.computations.sftp_credential_encryption import decrypt_existing_sftp_credentials

global apply_thresholds
def apply_thresholds(values_to_assign, rule_group_row):
    """
    Apply threshold and return values
    """
    try:
        logging.warning("Inside apply_thresholds")
        threshold_source_table = rule_group_row.iloc[0]['threshold_source_table']
        threshold_filter_column = rule_group_row.iloc[0]['threshold_filter_column']
        threshold_criteria = rule_group_row.iloc[0]['threshold_criteria']
        threshold_filter_value = rule_group_row.iloc[0]['threshold_filter_value']
        threshold_value = rule_group_row.iloc[0]['threshold_value']
        operation_parameter = rule_group_row.iloc[0]['operation_parameter']
        
        # skip applying threshold if not any of the threshold arguments are empty
        if pd.isna(threshold_source_table) or pd.isna(threshold_filter_column) or pd.isna(threshold_criteria) or pd.isna(threshold_filter_value) or pd.isna(threshold_value) or pd.isna(operation_parameter):
            logging.warning("Threshold parameters are not set, returning original values")
            return values_to_assign
        
        threshold_source_table, threshold_filter_column = threshold_source_table.strip(), threshold_filter_column.strip()
        # Apply threshold
        # Get the source table from source data
        if threshold_source_table not in source_data:
            logging.warning(f"Source data for '{threshold_source_table}' not available, returning original values.")
            return values_to_assign

        data = source_data[threshold_source_table]
        # check if threshold_filter_column exist in threshold_source_table
        if threshold_filter_column not in data.columns:
            logging.warning(f"threshold_filter_column: {threshold_filter_column} does not exist in {threshold_source_table}, returning original values.")
            return values_to_assign
            
        if threshold_criteria.upper() == 'EQUAL TO':
            cond = data[threshold_filter_column] == threshold_filter_value
        elif threshold_criteria.upper() == 'NOT EQUAL TO':
            cond = data[threshold_filter_column] != threshold_filter_value
        elif threshold_criteria.upper() == 'LESS THAN':
            cond = data[threshold_filter_column] < threshold_filter_value
        elif threshold_criteria.upper() == 'LESS THAN EQUAL TO':
            cond = data[threshold_filter_column] <= threshold_filter_value
        elif threshold_criteria.upper() == 'GREATER THAN':
            cond = data[threshold_filter_column] > threshold_filter_value
        elif threshold_criteria.upper() == 'GREATER THANEQUAL TO':
            cond = data[threshold_filter_column] >= threshold_filter_value
        else:
            logging.warning(f"Unknown threshold_criteria: {threshold_criteria}, returning original values")
            return values_to_assign
        
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


    if pd.isna(operation):
        logging.warning("Operation is empty. Returning original values.")
        logging.warning(f"  ")  #for testing
        return values
    
    if operation in ['ADD', 'SUBTRACT', 'MULTIPLY', 'DIVIDE'] and pd.isna(operation_value):
        logging.warning("Operation value cannot be empty for selected operation. Returning original values.")
        logging.warning(f"  ")  #for testing
        return values

    if pd.isna(source_data_type):
        logging.warning("Source data type is empty. Returning original values.")
        logging.warning(f"  ")  #for testing
        return values

    logging.warning("Applying datatype")
    try:
        if source_data_type.upper() == 'DATETIME':
            values = pd.to_datetime(values, format='%Y-%m-%d', errors='coerce')
            operation_value = pd.to_datetime(operation_value, format='%Y-%m-%d', errors='coerce')
        elif source_data_type.upper() == 'NUMERIC':
            values = pd.to_numeric(values, errors='coerce')
            operation_value = pd.to_numeric(operation_value, errors='coerce')
        elif source_data_type.upper() == 'CHARACTER':
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
        elif operation.upper() == 'UPPERCASE':
            values = values.str.upper()
        elif operation.upper() == 'LOWERCASE':
            values = values.str.lower()
        elif operation.upper() == 'TRIM':
            values = values.str.strip()
        elif operation.upper() == 'CAPITALIZE':
            values = values.str.capitalize()
        elif operation.upper() == 'TITLE':
            values = values.str.title()

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
        value_adjustment_operation = row['value_adjustment_operation']
        transformation_operation_value = row['transformation_operation_value']
        source_data_type = row['source_data_type']

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
            if not pd.isna(rule_group_operation):
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

                logging.warning("Applying datatype")
                if condition_datatype.upper() == 'DATETIME':
                    target_series = pd.to_datetime(target_series, format='%Y-%m-%d', errors='coerce')
                elif condition_datatype.upper() == 'NUMERIC':
                    target_series = pd.to_numeric(target_series, errors='coerce')
                elif condition_datatype.upper() == 'CHARACTER':
                    target_series = target_series.astype(str)
                else:
                    logging.warning(f"  ")  #for testing
                    logging.warning(f"Unknown condition_datatype: {source_data_type}")
                    logging.warning(f"  ")  #for testing
                    return 
                
            # Apply threshold if specified
            target_series = apply_thresholds(target_series, rule_group_rows)

            
            if len(rule_group_rows) == 1:
                final_target_series = target_series
            else:
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

global process_direct_mapping
def process_direct_mapping(row, source_data, target_data, rule_group_definition):
    """
    Processes direct mapping for a given row.
    """

    try:
        target_table_name = row['target_table_name']
        target_column_name = row['target_table_column']
        source_column_name = row['source_column_name']
        source_table_name = row['source_table_name']
        value_adjustment_operation = row['value_adjustment_operation']
        transformation_operation_value = row['transformation_operation_value']
        transformation_rule_reference = row['transformation_rule_reference']
        source_data_type = row['source_data_type']

        logging.warning(f"Processing direct mapping for target column '{target_column_name}' from source column '{source_column_name}'.")

        if source_table_name not in source_data:
            logging.warning(f"Source data for '{source_table_name}' not available. Skipping this mapping.")
            return

        data = source_data[source_table_name]
        if source_column_name not in data.columns:
            logging.warning(f"source_column_name:{source_column_name} does not exist. Skipping this mapping.")
            return

        if source_table_name not in target_data:
            target_data[source_table_name] = pd.DataFrame(index=data.index)

        source_column_name = source_column_name.strip() if not pd.isna(source_column_name) else source_column_name
        target_column_name = target_column_name.strip() if not pd.isna(target_column_name) else target_column_name

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
            logging.warning(f"Source data for '{source_table_name}' not available. Skipping this mapping.")
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
        master_name = master_mapping_row['master_name']
        master_source_column = master_mapping_row['master_source_column']
        master_target_column = master_mapping_row['master_target_column']
        logging.warning(f"Master mapping details: master_name='{master_name}', master_source_column='{master_source_column}', master_target_column='{master_target_column}'.")

        # Step 3: Get the actual mappings from master_repository using master_name
        master_data = master_repository[master_repository['master_name'] == master_name]
        if master_data.empty:
            logging.warning(f"No master data found for master table '{master_name}'.")
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
        logging.warning(f"Error in master mapping: {e} ")

global process_global_operation
def process_global_operation(row, source_data, target_data):
    """
    Processes global parameter mapping for a given row.
    """

    try:
        target_table_name = row['target_table_name']
        target_column_name = row['target_table_column']
        source_table_name = row.get('source_table_name', None)
        
        logging.warning(f"Processing global parameter mapping for target column '{target_column_name}' in table '{target_table_name}'.")

        if pd.isna(target_column_name):
            logging.warning("Target Column Value not defined. Skipping")
            return

        # Get the value from global_parameter DataFrame
        # If target_column_name is in available global variables, fill the column with the variable value
        if target_column_name.lower().replace(' ','_') == 'legal_entity':
            value = legal_entity
        elif target_column_name.lower().replace(' ','_') == 'reporting_date':
            value = reporting_date
        else:
            logging.warning(f"No matching global parameter found for column '{target_column_name}'. Skipping.")
            return

        # Now, assign this value to the target_data
        # Determine the number of rows

        num_rows = len(source_data[source_table_name])
        index = source_data[source_table_name].index

        # if target_table_name in target_data:
        #     # Use existing target_data[target_table_name]
        #     num_rows = len(target_data[target_table_name])
        #     index = target_data[target_table_name].index
        # elif target_table_name and source_table_name in source_data:
        #     # Use the index of the source_data[source_table_name]
        #     num_rows = len(source_data[source_table_name])
        #     index = source_data[source_table_name].index
        #     # Initialize the target_data[target_table_name]
        #     target_data[target_table_name] = pd.DataFrame(index=index)
        # else:
        #     # Cannot determine the number of rows
        #     logging.warning(f"Cannot determine the number of rows for target table '{target_table_name}'. Skipping.")
        #     return

        # Create a Series with the constant value
        values_to_assign = pd.Series([value] * num_rows, index=index)

        # Assign the value to the target column
        target_data[source_table_name][target_column_name] = values_to_assign.values

        logging.warning(f"Assigned global parameter value '{value}' to column '{target_column_name}' in table '{target_table_name}'.")

    except Exception as e:
        logging.warning(f"Error in global parameter mapping: {e}")

global process_transformations
def process_transformations(transformation_mapping, conditional_rules, master_repository, source_data, target_data, rule_group_definition, master_mapping_rule_definition):
    """
    Processes all the transformations as per the transformation mapping.
    """

    try:
        # Get unique source_table_names
        unique_source_tables = transformation_mapping['source_table_name'].unique()

        for source_table in unique_source_tables:

            # to run for specific table only
            # if source_table != 'market_debt_data': #replace with desired table name
            #     continue
            # if source_table not in ['indus_pos_data', 'pennant_pos_data']: #replace with desired table name
            #     continue

            logging.warning(f" ")
            logging.warning(f"Processing transformations for source table '{source_table}'.")

            # Filter the transformation mapping for this source_table
            transformations_for_source = transformation_mapping[transformation_mapping['source_table_name'] == source_table]

            # If source_table not in source_data, skip
            if source_table not in source_data:
                logging.warning(f"Source data for '{source_table}' not available. Skipping transformations for this source.")
                logging.warning(f"   ")
                logging.warning(f"   ")
                continue

            # Variable to run for specific row number of excel sheet
            # run_for = 10  # Set to desired row number
            

            # Process each transformation for this source_table
            for idx, row in transformations_for_source.iterrows():
                
                # if idx+2 != run_for:
                #     continue
                # if idx+2 not in [80, 123]:
                #     continue

                logging.warning(f"Prcoessing row:{idx+2}")
                transformation_type = row['transformation_type']
                
                if transformation_type == 'Direct Mapping':
                    # continue
                    process_direct_mapping(row, source_data, target_data, rule_group_definition)
                elif transformation_type == 'Master Mapping Operations':
                    # continue 
                    process_master_mapping(row, source_data, master_repository, target_data, rule_group_definition, master_mapping_rule_definition)
                elif transformation_type == 'Conditional Operations':
                    # continue
                    process_conditional_operation(row, source_data, conditional_rules, target_data, rule_group_definition)
                elif transformation_type == 'Global Parameter':
                    # continue
                    process_global_operation(row, source_data, target_data)
                else:
                    # continue
                    logging.warning(f"source table name { source_table} Unknown transformation type '{transformation_type}'. Skipping row {idx}.")
            
            import time
            time.sleep(5)   #enoughf time to read log properly.
            
    except Exception as e:
        logging.warning(f"Error processing transformations: {e}")
    
    logging.warning("   ")
        
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
            file_path_pq = os.path.join(parquet_error_free_path, f"{source_table_name}.parquet")
            
            if os.path.exists(file_path_pq):
                data = pd.read_parquet(file_path_pq)
                source_data[source_table_name] = data
            else:
                logging.warning(f"Source data file for '{source_table_name}' not found. Skipping this source.")
            
        except Exception as e:
            logging.warning(f"Error reading source data for '{source_table_name}': {e}")
    return source_data

global write_output_data
def write_output_data(target_data):
    """
    Writes the target tables to separate Excel files.
    """
    
    try:
        logging.warning("Writing output data to target parquet files.")
        
        # Create directories if not exist
        try:
            shutil.rmtree(parquet_transformed_path)
            os.makedirs(parquet_transformed_path)   
        except FileNotFoundError:
            os.makedirs(parquet_transformed_path)    
        except OSError as e:
            raise Exception(f"Error deleting Transformed Data directory: {e}")
            
        for table_name, data in target_data.items():
            df_save_path_parquet = os.path.join(parquet_transformed_path, f"{table_name}.parquet")
            logging.warning(f"  ")  #for testing
            logging.warning(f"Written data for target table '{table_name}' to '{df_save_path_parquet}'.")
            
            # write df to parquet
            temp_table = pa.Table.from_pandas(data)
            pq.write_table(temp_table, df_save_path_parquet)

            df_save_path = fr"/home/azureuser/DataFrameWork Testing/Latest/Transformed Data/{table_name}.csv"
            excel_buffer = BytesIO()
            data.to_csv(excel_buffer, index=False)
            excel_buffer.seek(0) 
            sftp_client.putfo(excel_buffer, df_save_path)

        logging.warning("Output data written successfully.")
    except Exception as e:
        logging.warning(f"Error writing output data: {e}")

global get_sftp_connection
def get_sftp_connection(Sftp_Name):
    """
    Returns:
    - sftp_client: Paramiko SFTP client object for Sftp_Name.
    - ssh_client: Paramiko SSH client object for Sftp_Name.
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


# Global Parameters
global reporting_date, legal_entity
reporting_date, legal_entity = Reporting_Date, Legal_Entity

global sftp_client, ssh_client
sftp_client, ssh_client = get_sftp_connection('Antares SFTP')

if sftp_client is None:
    raise Exception(f"Couldn't connect to SFTP: Antares SFTP")

global parquet_path, parquet_error_free_path, parquet_transformed_path
parquet_path = os.path.join(PLATFORM_FILE_PATH, "alm_data/Data Framework")
parquet_error_free_path = os.path.join(parquet_path, "Error Free Data")
parquet_transformed_path = os.path.join(parquet_path, "Transformed Data")

# Create directories if not exist
if not os.path.exists(parquet_error_free_path):
    raise Exception("No Error Free Data found to transform")

# Read Necessary tables
transformation_mapping = Data1
master_repository = Data2
conditional_rules = Data3
rule_group_definition = Data4
master_mapping_rule_definition =  Data5


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

