global datetime, pytz, PLATFORM_FILE_PATH
import pandas as pd
import logging
import os
from datetime import datetime
import pytz
from config.settings.base import PLATFORM_FILE_PATH
global report_name
report_name = Report_Name

logging.warning(f"Started generating aggregation report on Report Name: {report_name}")

global write_to_report_dict
def write_to_report_dict(table, aggregation_column, value, column, aggregation_type, raw_data_output=None, error_free_data_output=None, absolute_impact=None, percentage_impact=None):
    """
    Function to write report content to report dict
    """

    report['report_date'].append(datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S'))
    report['report_name'].append(report_name)
    report['source_table_name'].append(table)
    report['aggregation_column'].append(aggregation_column)
    report['aggregation_value'].append(value)
    report['operation_column'].append(column)
    report['aggregation_type'].append(aggregation_type)
    report['raw_data'].append(raw_data_output)
    report['error_free_data'].append(error_free_data_output)
    if absolute_impact is not None:
        report['absolute_impact'].append(f"{absolute_impact:.6f}")
    else:
        report['absolute_impact'].append(absolute_impact)
    if percentage_impact is not None:
        report['percentage_impact'].append(f"{percentage_impact:.6f}")
    else:
        report['percentage_impact'].append(percentage_impact)


global get_output_values
def get_output_values(table, raw_data, error_free_data, column, aggregation):
    """
    Apply specified aggregation opretion and returns the values: raw_data_output, error_free_data_output, absolute_impact, percentage_impact
    """

    if aggregation == "count":
        if 'repayment' in table:
            raw_data_output = len(raw_data['position_id'].unique())
            error_free_data_output = len(error_free_data['position_id'].unique())
        else:
            raw_data_output = len(raw_data)
            error_free_data_output = len(error_free_data)

    else:
        raw_data = raw_data[column]
        error_free_data = error_free_data[column]
        if aggregation == "sum":
            raw_data_output = raw_data.sum()
            error_free_data_output = error_free_data.sum()
        elif aggregation == "avg":
            raw_data_output = raw_data.mean()
            error_free_data_output = error_free_data.mean()
        elif aggregation == "min":
            raw_data_output = raw_data.min()
            error_free_data_output = error_free_data.min()
        elif aggregation == "max":
            raw_data_output = raw_data.max()
            error_free_data_output = error_free_data.max()
        else:
            raise Exception("Unknown Aggregation type: {aggregation}")
          
    # get difference of raw_data and error_free_Data
    absolute_impact = raw_data_output - error_free_data_output
    if absolute_impact:
        if aggregation in ['sum', 'avg', 'count']:
            # get difference percentage
            percentage_impact = (absolute_impact/raw_data_output)*100
        else:
            percentage_impact = None
    else:
        percentage_impact = 0
    return raw_data_output, error_free_data_output, absolute_impact, percentage_impact


global generate_report
def generate_report(agg_report_format):
    """
    Generated the Aggregation Report Based on Report Name selected in global variables.
    """
  
    logging.warning("Generating.....")
    agg_report_format = agg_report_format.groupby('source_table_name')
    for table, table_report_format in agg_report_format:
        
        logging.warning("   ")
        logging.warning(f"Processing table: {table}")

        try:
            # Read raw data
            parquet_file_path = os.path.join(parquet_raw_path,f"{table}.parquet")
            raw_data_df = pd.read_parquet(parquet_file_path)
            raw_data_df.columns = raw_data_df.columns.str.lower().str.replace(' ','_')
            # Read error free
            parquet_file_path = os.path.join(parquet_error_free_path,f"{table}.parquet")
            error_free_data_df = pd.read_parquet(parquet_file_path)
            error_free_data_df.columns = error_free_data_df.columns.str.lower().str.replace(' ','_')
        except FileNotFoundError:
            logging.warning("File not found, skipping")
            continue
          
        for i, row in table_report_format.iterrows():
            try:
                aggregation_column = row['aggregation_column']
                aggregation_value = row['aggregation_value']
                aggregation_type = row['aggregation_type']
            
                column = row['operation_column']
                # if column or aggregation type is empty, add empty values in report and continue
                if pd.isna(column):
                    logging.warning("Operation Column Name is empty, skipping")
                    write_to_report_dict(table, aggregation_column, aggregation_value,  column, aggregation_type)
                    continue
                if pd.isna(aggregation_type):
                    logging.warning("Aggregation type is empty")
                    write_to_report_dict(table, aggregation_column, aggregation_value,  column, aggregation_type)
                    continue
                
                column = column.lower().replace(' ','_')
                aggregation_type = row['aggregation_type'].lower()

                # if specified columns does not exist in data, add empty values in report and continue
                if (column not in raw_data_df.columns) and (column not in error_free_data_df.columns):
                    logging.warning("Operation Column not found")
                    write_to_report_dict(table, aggregation_column, aggregation_value, column, aggregation_type)
                    continue
                    
                if not pd.isna(aggregation_column):
                    write_to_report_dict(table, aggregation_column, aggregation_value, column, aggregation_type)
                    aggregation_column = aggregation_column.lower().replace(' ','_')
          
                # if aggregation column and aggregation indentifier value both are defined
                if not pd.isna(aggregation_column) and not pd.isna(aggregation_value):
                    logging.warning(f"Processing {aggregation_type} on column: {column} by aggregating: {aggregation_column} and value: {aggregation_value}")
                    if aggregation_column in raw_data_df.columns and aggregation_column in error_free_data_df:
                        raw_data = raw_data_df.loc[raw_data_df[aggregation_column] == aggregation_value, column]
                        error_free_data = error_free_data_df.loc[error_free_data_df[aggregation_column] == aggregation_value, column]
                        
                        # get output and store into report
                        raw_data_output, error_free_data_output, absolute_impact, percentage_impact = get_output_values(table, raw_data, error_free_data, column, aggregation_type)
                        write_to_report_dict(table, aggregation_column, value, column, aggregation_type, raw_data_output, error_free_data_output, absolute_impact, percentage_impact)

                # if only aggregation column in defined        
                elif not pd.isna(aggregation_column):
                    logging.warning(f"Processing {aggregation_type} on column: {column} by aggregating: {aggregation_column}")
                    if aggregation_column in raw_data_df.columns and aggregation_column in error_free_data_df:
                        raw_data_agg_column_unique_values = raw_data_df[aggregation_column].unique()
                        error_free_data_agg_column_unique_values = error_free_data_df[aggregation_column].unique()
                        for value in raw_data_agg_column_unique_values:
                            raw_data = raw_data_df.loc[raw_data_df[aggregation_column] == value, column]
                            error_free_data = error_free_data_df.loc[error_free_data_df[aggregation_column] == value, column]

                            # get output and store into report
                            raw_data_output, error_free_data_output, absolute_impact, percentage_impact = get_output_values(table, raw_data, error_free_data, column, aggregation_type)
                            write_to_report_dict(table, aggregation_column, value, column, aggregation_type, raw_data_output, error_free_data_output, absolute_impact, percentage_impact)

                # if only operation column and aggregation type is defined        
                else:
                    # get output and store into report
                    logging.warning(f"Processing {aggregation_type} on column: {column}")
                    raw_data_output, error_free_data_output, absolute_impact, percentage_impact = get_output_values(table, raw_data_df, error_free_data_df, column, aggregation_type)
                    write_to_report_dict(table, aggregation_column, aggregation_value, column, aggregation_type, raw_data_output, error_free_data_output, absolute_impact, percentage_impact)
                  
            except Exception as e:
                raise Exception(f"Error {e}")
            
            logging.warning("   ")


# Main Code
agg_report_format = Data1 

if agg_report_format.empty:
    logging.warning("Report Name not found in Aggregation Report Config Table.")
    raise Exception("Report Name not found in Aggregation Report Config Table.")

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

global parquet_path, parquet_raw_path, parquet_error_free_path
parquet_path = os.path.join(PLATFORM_FILE_PATH, "alm_data/Data Framework")
parquet_raw_path = os.path.join(parquet_path, "Raw Data")
parquet_error_free_path = os.path.join(parquet_path, "Error Free Data")

if not os.path.exists(parquet_raw_path):
    logging.warning("No Raw Data found to generate report")
    raise Exception("No Raw Data found to generate report")

if not os.path.exists(parquet_error_free_path):
    logging.warning("No Error Free Data found to generate report")
    raise Exception("No Error Free Data found to generate report")

generate_report(agg_report_format)
report = pd.DataFrame(report)
report['absolute_impact'] = pd.to_numeric(report['absolute_impact'], errors='coerce')
report['percentage_impact'] = pd.to_numeric(report['percentage_impact'], errors='coerce')
output_data = report.astype(str)

logging.warning('Aggregation Completed')
