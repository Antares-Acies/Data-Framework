"""
aggregation_report.py
This script generates an aggregation report based on the configuration settings specified in the aggregation_report_config table.
The report consolidates data from various sources and provides a comprehensive overview as per the defined parameters.
"""

global datetime, pytz

import pandas as pd
import logging
from io import BytesIO
import os
from datetime import datetime
import pytz

# Log the start of the report generation process
logging.warning(f"Started generating aggregation report on Report Name: {Report_Name}")

# Load the aggregation report format from the configuration table
agg_report_format = Data1 

# Check if the report format is empty
if agg_report_format.empty:
    logging.warning("Report Name not found in Aggregation Report Config Table.")
    raise Exception("Report Name not found in Aggregation Report Config Table.")

# Initialize the report dictionary to store the results
global report
report = {
    "report_name": [],
    "source_table_name": [],
    "aggregation_column": [],
    "aggregation_value": [],
    "operation_column": [],
    "aggregation_type": [],
    "raw_data": [],
    "error_free_data": [],
    "absolute_impact": [],
    "percentage_impact": [],
    "report_date": []
}

# Define paths for raw and error-free data
global parquet_path, parquet_raw_path, parquet_error_free_path
parquet_path = r"/opt/revolutio/Platform_Configs/alm_data/Data Framework"
parquet_raw_path = os.path.join(parquet_path, "Raw Data")
parquet_error_free_path = os.path.join(parquet_path, "Error Free Data")

# Check if the raw data path exists
if not os.path.exists(parquet_raw_path):
    logging.warning("No Raw Data found to generate report")
    raise Exception("No Raw Data found to generate report")

# Check if the error-free data path exists
if not os.path.exists(parquet_error_free_path):
    logging.warning("No Error Free Data found to generate report")
    raise Exception("No Error Free Data found to generate report")

# Function to generate the aggregation report
global generate_report
def generate_report(agg_report_format):
    """
    Generates the Aggregation Report based on the Report Name selected in global variables.
    
    Parameters:
    agg_report_format (DataFrame): The configuration settings for the aggregation report.
    
    Expected Output:
    A DataFrame containing the aggregation report with columns for report name, source table name,
    aggregation column, aggregation value, operation column, aggregation type, raw data, error-free data,
    absolute impact, percentage impact, and report date.
    
    Side Effects:
    Logs warnings and raises exceptions if there are issues with the data paths or aggregation columns.
    """
  
    logging.warning("Generating.....")
    agg_report_format = agg_report_format.groupby('source_table_name')
    for table, table_report_format in agg_report_format:
        
        logging.warning(f"Processing table: {table}")
        
        # Read raw data
        parquet_file_path = os.path.join(parquet_raw_path, f"{table}.parquet")
        raw_data_df = pd.read_parquet(parquet_file_path)

        # Read error-free data
        parquet_file_path = os.path.join(parquet_error_free_path, f"{table}.parquet")
        error_free_data_df = pd.read_parquet(parquet_file_path)

        for i, row in table_report_format.iterrows():
  
            aggregation_column = row['aggregation_column']
            aggregation_value = row['aggregation_value']
            column = row['operation_column']
            agg = row['aggregation_type'].lower()

            report['report_name'].append(row['report_name'])
            report['source_table_name'].append(table)
            report['aggregation_column'].append(aggregation_column)
            report['aggregation_value'].append(aggregation_value)
            report['operation_column'].append(column)
            report['aggregation_type'].append(agg)
            report['report_date'].append(datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S'))
          
            logging.warning(f"Processing: {agg} on Column: {column} By aggregating: {aggregation_column} on {aggregation_value}")
          
            try:
                agg_raw_data = raw_data_df.loc[raw_data_df[aggregation_column] == aggregation_value, column]
                agg_error_free_data = error_free_data_df.loc[error_free_data_df[aggregation_column] == aggregation_value, column]

                if agg == "sum":
                    raw_data = agg_raw_data.sum()
                    error_free_data = agg_error_free_data.sum()
                elif agg == "avg":
                    raw_data = agg_raw_data.mean()
                    error_free_data = agg_error_free_data.mean()
                elif agg == "min":
                    raw_data = agg_raw_data.min()
                    error_free_data = agg_error_free_data.min()
                elif agg == "max":
                    raw_data = agg_raw_data.max()
                    error_free_data = agg_error_free_data.max()
                elif agg == "count":
                    raw_data = len(agg_raw_data)
                    error_free_data = len(agg_error_free_data)
                
                absolute_impact = raw_data - error_free_data
                if absolute_impact:
                    if agg in ['sum', 'avg', 'count']:
                        percentage_impact = (absolute_impact / raw_data) * 100
                    else:
                        percentage_impact = ""
                else:
                    percentage_impact = 0
                  
            except KeyError:
                logging.warning(f"Error in accessing aggregation/operation column value: '{aggregation_column}' not found.")
                raw_data, error_free_data, absolute_impact, percentage_impact = [""] * 4
            except Exception as e:
                logging.warning(f"Error in accessing aggregation/operation column value: '{aggregation_column}' {e}")
                raw_data, error_free_data, absolute_impact, percentage_impact = [""] * 4

            report['raw_data'].append(raw_data)
            report['error_free_data'].append(error_free_data)
            report['absolute_impact'].append(absolute_impact)
            report['percentage_impact'].append(percentage_impact)
        
        logging.warning("   ")

# Generate the report
generate_report(agg_report_format)
report = pd.DataFrame(report)
output_data = report.astype(str)