

# -*- coding: utf-8 -*-
"""
Created on Tue Dec  3 14:39:07 2024

@author: KumarAkashdeep
"""

import logging
import pandas as pd
import os

def read_configurations(config_file):
    """
    Reads the configuration Excel file and returns the necessary DataFrames.
    """
    try:
        logging.warning("Reading configuration file.")
        transformation_mapping = pd.read_excel(config_file, sheet_name='transformation_mapping')
        logging.warning("Read transformation mapping.")
        master_repository = pd.read_excel(config_file, sheet_name='master_repository')
        logging.warning("Read master repository.")
        conditional_rules = pd.read_excel(config_file, sheet_name='conditional_rule_definition')
        logging.warning("Read conditional rule definition.")
        rule_group_definition = pd.read_excel(config_file, sheet_name='rule_group_definition')
        logging.warning("Read rule group definition.")
        master_mapping_rule_definition = pd.read_excel(config_file, sheet_name='master_mapping_rule_definition')
        logging.warning("Read master mapping rule definition.")
        global_parameter = pd.read_excel(config_file, sheet_name='global_parameter')
        logging.warning("Read global parameter")
        
        
        logging.warning("Configuration file read successfully.")
        logging.warning(f"  ")  #for testing
        logging.warning(f"  ")  #for testing
        logging.warning(f"  ")  #for testing
        return transformation_mapping, conditional_rules, master_repository, rule_group_definition, master_mapping_rule_definition, global_parameter
        
    except Exception as e:
        logging.warning(f"Error reading configuration file: {e}")
        # Error handled gracefully without raising an exception
        return None, None, None, None, None, None  # Return None if error occurs

def apply_value_adjustment(values, operation, operation_value):
    """
    Applies value adjustment operation to a pandas Series.
    If either operation or operation_value is None, returns the input values without transformation.
    """
    logging.warning(f"operation {operation} ")
    logging.warning(f"operation_value {operation_value} ")
    # logging.warning(f"values {values} ")



    if pd.isna(operation) or pd.isna(operation_value):
        logging.warning("Operation or operation_value is None or NaN. Returning original values.")
        logging.warning(f"  ")  #for testing
        return values
    
    if operation is None or operation_value is None:
        logging.warning("Operation or operation_value is None. Returning input values without transformation.")
        logging.warning(f"  ")  #for testing
        return values


    try:
        if operation.upper() == 'DATETIME':
            # Map the format string
            format_string = operation_value.replace('YYYY', '%Y').replace('MM', '%m').replace('DD', '%d')
            return pd.to_datetime(values, format=format_string, errors='coerce')
        elif operation.upper() == 'FLOAT':
            return pd.to_numeric(values, errors='coerce')
        elif operation.upper() == 'DATE':
            format_string = operation_value.replace('YYYY', '%Y').replace('MM', '%m').replace('DD', '%d')
            return pd.to_datetime(values, format=format_string, errors='coerce').dt.date
        elif operation.upper() == 'STRING':
            return values.astype(str)
        else:
            logging.warning(f"  ")  #for testing
            logging.warning(f"Unknown value adjustment operation: {operation}")
            logging.warning(f"  ")  #for testing
            return values
    except Exception as e:
        logging.warning(f"Error applying value adjustment: {e}")
        return values


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

        logging.warning(f"Processing conditional operation for target column '{target_column_name}' using transformation rule reference '{transformation_rule_reference}'.")

        # Get the rule_set from rule_group_definition
        rule_group_row = rule_group_definition[rule_group_definition['rule_group'] == transformation_rule_reference]
        if rule_group_row.empty:
            logging.warning(f"No rule set found for rule group '{transformation_rule_reference}'.")
            return

        rule_set = rule_group_row.iloc[0]['rule_set']
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
            condition_operation = cond_row['condition_operation']
            condition_source_table = cond_row['condition_source_table']
            condition_column_name = cond_row['condition_column_name']
            condition_datatype = cond_row['condition_datatype']
            condition_type = cond_row['condition_type']
            target_column_value = cond_row['target_column_value']
            target_column_value_source = cond_row['target_column_value_source']

            # Get the condition_source_table data
            if condition_source_table not in source_data:
                logging.warning(f"Condition source data for '{condition_source_table}' not available. Skipping this condition.")
                continue

            cond_data = source_data[condition_source_table]

            # Evaluate the condition
            if condition_column_name not in cond_data.columns:
                logging.warning(f"Condition column '{condition_column_name}' not found in '{condition_source_table}'. Skipping this condition.")
                continue

            # Get the column to evaluate
            condition_column = cond_data[condition_column_name]

            # Apply value adjustment to condition_column if needed
            # For now, we proceed without data type conversion for the condition column
            logging.warning(f"condition_type {condition_type} ")
            if condition_type == 'EMPTY':
                cond = condition_column.isnull() | (condition_column == '')
            elif condition_type == 'NOT EMPTY':
                cond = ~(condition_column.isnull() | (condition_column == ''))
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
                values_to_assign = apply_value_adjustment(values_to_assign, value_adjustment_operation, transformation_operation_value)

            # Assign the values where the condition is True
            target_series.loc[cond] = values_to_assign.loc[cond]

        if target_table_name not in target_data:
            target_data[target_table_name] = pd.DataFrame(index=data.index)

        # Assign the target_series to the target column
        logging.warning(f"target_series {target_series} ")
        target_data[target_table_name][target_column_name] = target_series

        logging.warning(f"Conditional operation completed for target column '{target_column_name}'.")

    except Exception as e:
        logging.warning(f"Error in conditional operation: {e}")
        # Error handled gracefully without raising an exception

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

        logging.warning(f"Processing direct mapping for target column '{target_column_name}' from source column '{source_column_name}'.")

        if source_table_name not in source_data:
            # logging.warning(f"Source data for '{source_table_name}' not available. Skipping this mapping.")
            return

        data = source_data[source_table_name]
        if target_table_name not in target_data:
            target_data[target_table_name] = pd.DataFrame(index=data.index)

        values_to_assign = data[source_column_name]

        # Apply value adjustment if specified
        if value_adjustment_operation and transformation_operation_value:
            values_to_assign = apply_value_adjustment(values_to_assign, value_adjustment_operation, transformation_operation_value)

        target_data[target_table_name][target_column_name] = values_to_assign

    except Exception as e:
        logging.warning(f"Error in direct mapping: {e}")
        # Error handled gracefully without raising an exception

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

        if target_table_name not in target_data:
            target_data[target_table_name] = pd.DataFrame(index=data.index)

        # Apply the mapping to the source column
        source_values = data[source_column_name].map(mapping_dict)
        target_data[target_table_name][target_column_name] = source_values
    except Exception as e:
        logging.warning(f"Error in master mapping: {e}")
        # Error handled gracefully without raising an exception

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

        if target_table_name in target_data:
            # Use existing target_data[target_table_name]
            num_rows = len(target_data[target_table_name])
            index = target_data[target_table_name].index
        elif source_table_name and source_table_name in source_data:
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
        target_data[target_table_name][target_column_name] = values_to_assign.values

        logging.warning(f"Assigned global parameter value '{value}' to column '{target_column_name}' in table '{target_table_name}'.")

    except Exception as e:
        logging.warning(f"Error in global parameter mapping: {e}")
        # Error handled gracefully without raising an exception

def process_transformations(transformation_mapping, conditional_rules, master_repository, source_data, target_data, rule_group_definition, master_mapping_rule_definition, global_parameter):
    """
    Processes all the transformations as per the transformation mapping.
    """
    try:
        # Get unique source_table_names
        unique_source_tables = transformation_mapping['source_table_name'].unique()
        # Variable to control outer loop
        break_outer = False

        for source_table in unique_source_tables:
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
                transformation_type = row['transformation_type']

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
        # Error handled gracefully without raising an exception
        
def read_source_data(source_table_names):
    """
    Reads source data from individual files, supporting Excel and CSV formats.
    """
    source_data = {}
    source_folder = r"C:\Users\KumarAkashdeep\Downloads\data configuration"  # Update the path to your actual source data folder
    for source_table_name in source_table_names:
        try:
            logging.warning(f"Reading source data for '{source_table_name}'.")
            # File extensions to search for
            file_found = False
            for ext in ['.xlsb', '.xlsx', '.xls', '.csv']:
                file_path = os.path.join(source_folder, f"{source_table_name}{ext}")
                if os.path.exists(file_path):
                    if ext == '.xlsb':
                        data = pd.read_excel(file_path, engine='pyxlsb')
                    elif ext == '.csv':
                        data = pd.read_csv(file_path)  # Use read_csv for CSV files
                    else:
                        data = pd.read_excel(file_path)
                    source_data[source_table_name] = data
                    logging.warning(f"Read data for '{source_table_name}' from '{file_path}'.")
                    file_found = True
                    break
            if not file_found:
                logging.warning(f"Source data file for '{source_table_name}' not found. Skipping this source.")
        except Exception as e:
            logging.warning(f"Error reading source data for '{source_table_name}': {e}")
            # Error handled gracefully without raising an exception
    return source_data

def write_output_data(target_data):
    """
    Writes the target tables to separate Excel files.
    """
    try:
        logging.warning("Writing output data to target Excel files.")
        output_folder = 'output_data'  # Replace with your desired output folder path
        os.makedirs(output_folder, exist_ok=True)
        for table_name, data in target_data.items():
            output_file = os.path.join(output_folder, f"{table_name}.xlsx")
            logging.warning(f"  ")  #for testing
            logging.warning(f"Writing data for target table '{table_name}' to '{output_file}'.")
            data.to_excel(output_file, index=False)
        logging.warning("Output data written successfully.")
    except Exception as e:
        logging.warning(f"Error writing output data: {e}")
        # Error handled gracefully without raising an exception

# Read configurations
config_file = r"C:\Users\KumarAkashdeep\Downloads\data configuration\Antares Data Framework.xlsx"
transformation_mapping, conditional_rules, master_repository, rule_group_definition, master_mapping_rule_definition, global_parameter = read_configurations(config_file)

# Check if configurations were read successfully
if transformation_mapping is None:
    logging.warning("Failed to read configurations. Exiting program.")
else:
    # Get unique source table names
    source_table_names = transformation_mapping['source_table_name'].unique()

    # Read source data
    source_data = read_source_data(source_table_names)

    # Create empty target_data dictionary
    target_data = {}

    # Process transformations
    process_transformations(transformation_mapping, conditional_rules, master_repository, source_data, target_data, rule_group_definition, master_mapping_rule_definition, global_parameter)

    # Write output data
    write_output_data(target_data)
