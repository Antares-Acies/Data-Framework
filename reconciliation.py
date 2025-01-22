global os, logging
import pandas as pd
import os
import logging

# Log the start of the reconciliation report generation process
logging.warning(f"Started generating aggregation report on Report Name")

# Load the aggregation report format from the configuration table
recon_report_format = Data1 

global parquet_path, parquet_error_free_path, parquet_transformed_path
parquet_path = r"/opt/revolutio/Platform_Configs/alm_data/Data Framework"
parquet_error_free_path = os.path.join(parquet_path, "Error Free Data")
parquet_transformed_path = os.path.join(parquet_path, "Transformed Data")

# Check if the report format is empty
if recon_report_format.empty:
    logging.warning("Report Name not found in Aggregation Report Config Table.")
    raise Exception("Report Name not found in Aggregation Report Config Table.")

# Initialize the report dictionary to store the results
global report
report = {
    "source_name": [],
    "source_table_name": [],
    "aggregation_level": [],
    "aggregation_identifier": [],
    "source_total": [],
    "target_total": [],
    "difference": [],
}

# Check if the error-free data path exists
if not os.path.exists(parquet_error_free_path):
    logging.warning("No Error Free Data found to generate report")
    raise Exception("No Error Free Data found to generate report")

# Check if the transformed data path exists
if not os.path.exists(parquet_transformed_path):
    logging.warning("No Transformed Data found to generate report")
    raise Exception("No Transformed Data found to generate report")

# Function to generate the reconciliation report
global generate_report
def generate_report(recon_report_format):
    
    logging.warning("Generating.....")

    for i, row in recon_report_format.iterrows():

        logging.warning(f"Processing row: {i+1}")
        source_name = row['source_name']

        aggregation_level = row['aggregation_level']

        source_table_name = row['source_table_name']
        source_aggregation_column = row['source_aggregation_column']
        source_value_column = row['source_value_column']

        target_table_name = row['target_table_name']
        target_aggregation_column = row['target_aggregation_column']
        target_value_column = row['target_value_column']
        
        logging.warning(f"Processing Aggregation: {aggregation_level} on Source Table: {source_table_name}, Source Aggregation Column: {source_aggregation_column}, Source Value Column {source_value_column}")
        
        try:
            # Read error-free data
            parquet_file_path = os.path.join(parquet_error_free_path, f"{source_table_name}.parquet")
            error_free_data_df = pd.read_parquet(parquet_file_path)

            # Read transformed data
            parquet_file_path = os.path.join(parquet_transformed_path, f"{source_table_name}.parquet")
            transformed_data_df = pd.read_parquet(parquet_file_path)
            logging.warning(f"Read Error_free and transformed data")
        except Exception as e:
            logging.warning(f"Error reading Error_free/transformed data: {e}")
            continue

        # Get Source Data Total
        if (source_aggregation_column in error_free_data_df.columns) and (source_value_column in error_free_data_df.columns):
            error_free_unique_aggregation_values = error_free_data_df[source_aggregation_column].unique()
            count_error_free_unique_aggregation_values = len(error_free_unique_aggregation_values)
           
            for identifier in error_free_unique_aggregation_values:
                source_total = error_free_data_df.loc[
                                                    error_free_data_df[source_aggregation_column] == identifier, 
                                                    source_value_column
                                                ].sum()
                report["aggregation_identifier"].append(identifier)
                report["source_total"].append(source_total)
        else:
            count_error_free_unique_aggregation_values = 0
            if source_aggregation_column not in error_free_data_df.columns:
                logging.warning(f"{source_aggregation_column} does not exist in Error Free Data")
                raise Exception(f"{source_aggregation_column} does not exist in Error Free Data")
            else:
                logging.warning(f"{source_value_column} does not exist in Error Free Data")
                raise Exception(f"{source_value_column} does not exist in Error Free Data")
                
        # Get Target Data Total
        if (target_aggregation_column in transformed_data_df.columns) and (target_value_column in transformed_data_df.columns):
            trans_unique_aggregation_values = transformed_data_df[target_aggregation_column].unique()
            count_trans_unique_aggregation_values = len(trans_unique_aggregation_values)

            for identifier in trans_unique_aggregation_values:
                total = transformed_data_df.loc[
                                                    transformed_data_df[target_aggregation_column] == identifier, 
                                                    target_value_column
                                                ].sum()
                report["aggregation_identifier"].append(identifier)
                report["target_total"].append(total)
        else:
            count_trans_unique_aggregation_values = 0
            if target_aggregation_column not in transformed_data_df.columns:
                logging.warning(f"{target_aggregation_column} does not exist in Transformed Data")
                raise Exception(f"{target_aggregation_column} does not exist in Transformed Data")
            else:
                logging.warning(f"{target_value_column} does not exist in Transformed Data")
                raise Exception(f"{target_value_column} does not exist in Transformed Data:{transformed_data_df.columns}")
                
        if count_error_free_unique_aggregation_values or count_trans_unique_aggregation_values:
            max_count = max(count_error_free_unique_aggregation_values, count_trans_unique_aggregation_values)
        else:
            logging.warning(f"{source_aggregation_column} and {target_aggregation_column} are empty.")
            continue

        report['source_name'].append(source_name)
        report['source_table_name'].append(source_table_name)
        report['aggregation_level'].append(aggregation_level)
        report['source_name'].extend([source_name] * max_count)
        report['source_table_name'].extend([source_table_name] * max_count)
        report['aggregation_level'].extend([aggregation_level] * max_count)

        report['difference'] = report['source_total'] - report['target_total']


# Generate the report
generate_report(recon_report_format)
# data = [(i, len(v)) for i,v in report.items()]
# raise Exception(data)
report = pd.DataFrame(report)
output_data = report.astype(str)