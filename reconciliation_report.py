global datetime, pytz, PLATFORM_FILE_PATH
import pandas as pd
import logging
import os
from datetime import datetime
import pytz
from config.settings.base import PLATFORM_FILE_PATH
logging.warning(f"Started generating reconciliation report")

global write_to_report_dict
def write_to_report_dict(source_name, source_table_name, aggregation_level, aggregation_identifier=None, source_total=None, target_total=None, difference=None):
    """
    Writes output details to output report dict
    """

    report['reporting_date'].append(datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S'))
    report['source_name'].append(source_name)
    report['source_table_name'].append(source_table_name)
    report['aggregation_level'].append(aggregation_level)
    report['aggregation_identifier'].append(aggregation_identifier)
    report['source_total'].append(source_total)
    report['target_total'].append(target_total)
    report['difference'].append(difference)


global generate_report
def generate_report(recon_report_format):
    """
    Generated the Report.
    """
  
    logging.warning("Generating.....")
    recon_report_format = recon_report_format.groupby('source_table_name')

    # iterate in table group
    for table, table_report_format in recon_report_format:
        
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
            # Read transformed data
            parquet_file_path = os.path.join(parquet_transformed_path,f"{table}.parquet")
            transformed_data_df = pd.read_parquet(parquet_file_path)
            transformed_data_df.columns = transformed_data_df.columns.str.lower().str.replace(' ','_')
        except FileNotFoundError as e:
            logging.warning(f"File not found:{e}, skipping")
            continue
        except Exception as e:
            logging.warning(f"Error while reading file:{e}, skipping report.")
            continue
          
        # iterate in one table related rows 
        for i, row in table_report_format.iterrows():
            try:
                source_name = row['source_name']
                aggregation_level = row['aggregation_level']
                source_aggregation_column = row['source_aggregation_column']
                source_value_column = row['source_value_column']
                target_table_name = row['target_table_name']
                target_aggregation_column = row['target_aggregation_column']
                target_value_column = row['target_value_column']
            
                # if any of required column is empty, add empty values in report and continue
                if pd.isna(source_aggregation_column):
                    logging.warning("source_aggregation_column is empty, skipping report")
                    write_to_report_dict(source_name, table, aggregation_level)
                    continue
                if pd.isna(source_value_column):
                    logging.warning("source_value_column is empty, skipping report")
                    write_to_report_dict(source_name, table, aggregation_level)
                    continue
                if pd.isna(target_table_name):
                    logging.warning("target_table_name is empty, skipping report")
                    write_to_report_dict(source_name, table, aggregation_level)
                    continue
                if pd.isna(target_aggregation_column):
                    logging.warning("target_aggregation_column is empty, skipping report")
                    write_to_report_dict(source_name, table, aggregation_level)
                    continue
                if pd.isna(target_value_column):
                    logging.warning("target_value_column is empty, skipping report")
                    write_to_report_dict(source_name, table, aggregation_level)
                    continue
                
                # convert column names to system compatible names, to avoid column not found errors
                source_aggregation_column = source_aggregation_column.lower().replace(' ','_')
                target_aggregation_column = target_aggregation_column.lower().replace(' ','_')
                source_value_column = source_value_column.lower().replace(' ','_')
                target_value_column = target_value_column.lower().replace(' ','_')

                # if any of column does not exist in data, add empty values in report and continue
                if (source_aggregation_column not in raw_data_df.columns) and (source_value_column not in raw_data_df.columns):
                    logging.warning("Columns defined for source not found")
                    write_to_report_dict(source_name, table, aggregation_level)
                    continue
                if (target_aggregation_column not in transformed_data_df.columns) and (source_value_column not in transformed_data_df.columns):
                    logging.warning("Columns defined for target not found")
                    write_to_report_dict(source_name, table, aggregation_level)
                    continue

                # get source_total, target_total and differene  
                logging.warning(f"Processing aggregation_level: {aggregation_level} ")
                raw_data_agg_column_unique_values = raw_data_df[source_aggregation_column].unique()
                transformed_data__agg_column_unique_values = transformed_data_df[target_aggregation_column].unique()

                # get source_total, target_total and difference for each identifier of aggregation column mentioned
                for aggregation_identifier in raw_data_agg_column_unique_values:

                    raw_data_df[source_aggregation_column] = pd.to_numeric(raw_data_df[source_aggregation_column], errors='coerce')
                    source_total = raw_data_df.loc[raw_data_df[source_aggregation_column] == aggregation_identifier, source_value_column].sum()

                    transformed_data_df[target_aggregation_column] = pd.to_numeric(transformed_data_df[target_aggregation_column], errors='coerce')
                    target_total = transformed_data_df.loc[transformed_data_df[target_aggregation_column] == aggregation_identifier, target_value_column].sum()

                    difference = target_total - source_total

                    write_to_report_dict(source_name, table, aggregation_level, aggregation_identifier, source_total, target_total, difference)
                  
            except Exception as e:
                logging.warning(f"Error generating report: {e}")

# Main Code
recon_report_format = Data1 

if recon_report_format.empty:
    logging.warning("Reconciliation Report Format is empty")
    raise Exception("Reconciliation Report Format is empty")

global report
report = {
    "reporting_date": [],
    "source_name": [],
    "source_table_name": [],
    "aggregation_level": [],
    "aggregation_identifier": [],
    "source_total": [],
    "target_total": [],
    "difference": []
}

global parquet_path, parquet_raw_path, parquet_error_free_path, parquet_transformed_path
parquet_path = os.path.join(PLATFORM_FILE_PATH, "alm_data/Data Framework")
parquet_raw_path = os.path.join(parquet_path, "Raw Data")
parquet_error_free_path = os.path.join(parquet_path, "Error Free Data")
parquet_transformed_path = os.path.join(parquet_path, "Transformed Data")

if not os.path.exists(parquet_raw_path):
    logging.warning("No Raw Data found to generate report")
    raise Exception("No Raw Data found to generate report")

if not os.path.exists(parquet_error_free_path):
    logging.warning("No Error Free Data found to generate report")
    raise Exception("No Error Free Data found to generate report")

if not os.path.exists(parquet_transformed_path):
    logging.warning("No Transformed Data found to generate report")
    raise Exception("No Transformed Data found to generate report")

generate_report(recon_report_format)

report = pd.DataFrame(report)
output_data = report.astype(str)
logging.warning(f'Aggregation Completed')
