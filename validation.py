import logging
global create_engine, inspect
import pandas as pd
import logging
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import URL
import os
global BytesIO
from io import BytesIO
import paramiko
from datetime import datetime
global pa, pq
import pyarrow as pa
import pyarrow.parquet as pq

global log_error
def log_error(system_name=None, stagingdb_name=None, 
              check_type=None, product_variant=None, position_id=None, dq_column_name=None, 
              error_message=None, error_type=None, total_records=None):
    """
    Log the error in validation_error dictionary.
    """
    validation_error['extract_date'].append(extract_date)
    validation_error['dq_run_date'].append(dq_run_date)
    validation_error['system_name'].append(system_name)
    validation_error['stagingdb_name'].append(stagingdb_name)
    validation_error['check_type'].append(check_type)
    validation_error['product_variant'].append(product_variant)
    validation_error['position_id'].append(position_id)
    validation_error['dq_column_name'].append(dq_column_name)
    validation_error['error_message'].append(error_message)
    validation_error['error_type'].append(error_type)
    validation_error['total_records'].append(total_records)

# need to work db part (incomplete)
global get_db_connection
def get_db_connection(database):
    """
    Get DB Connection.
    """
    db_creds = config_file.parse("data_framework_db_creds")
    db_details = db_creds[db_creds['database_name'] == database].iloc[0]

    if db_details.empty:
        logging.warning(f"DB:{database} details not found in data_framework_sftp_creds")
        return 
    
    # setting driver according to db_type
    db_type = db_details['database_type'].lower()
    if db_type == "mssql":
        driver = "mssql+pyodbc"
    elif db_type == "postgresql":
        driver = "postgresql+psycopg2"
    elif db_type == "oracle":
        driver = "oracle+oracledb"

    schema = db_details['schema'].lower()

    connection_url = URL.create(
        driver,
        username=db_details['user_name'],
        password=db_details['password'],
        host=db_details['server_name'],
        port=db_details['port'],
        database=database,
    )

    try:
        logging.warning(f"Connecting to the Database: {database} ... ") 
        engine = create_engine(connection_url)
        connection = engine.connect()
        inspector = inspect(engine)
        logging.warning("Connected to the database")
        return engine, connection, schema, inspector
    except Exception as e:
        logging.warning(f"Exception in connecting to the database:{e}")
        log_error(error_message = f"Couldn't connect to database: {database}")
        return None, None, None, None

global get_sftp_client
def get_sftp_client(sftp):
    """
    Get SFTP Connection.
    """

    sftp_creds = config_file.parse("data_framework_sftp_creds")
    sftp_creds.columns = sftp_creds.columns.str.strip()

    try:
        sftp_details = sftp_creds[sftp_creds['sftp_name'] == sftp].iloc[0]
    except IndexError as e:
        log_error(error_message = f"{sftp} not found in data_framework_sftp_creds")
        logging.warning(f"{sftp} not found in data_framework_sftp_creds")
        return
    
    hostname = sftp_details['hostname'].strip()
    username = sftp_details['username'].strip()
    password = sftp_details['password'].strip()
    port = int(sftp_details['port'])
    path = sftp_details['path'].strip()

    # if path not given, skip
    if path is None:
        log_error(error_message = f"SFTP Path not given, Skipped Validation for source: {sftp}")
        logging.warning(f"path not given")
        logging.warning(f"Skipped Validation for source: {sftp}")
        return 

    # Ensure path ends with a slash
    if not path.endswith('/'):
        path += '/'

    try:

        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        logging.warning(f"Connecting to the SFTP server {sftp} ...")
        ssh_client.connect(hostname=hostname, username=username, password=password, port=port, timeout=30)
        sftp_client = ssh_client.open_sftp()
        logging.warning(f"Connected to the server")
        return sftp_client, path  
        
    except Exception as e:

        log_error(error_message = f"Couldn't connect to server: {sftp}")
        logging.warning(f"Exception in connecting to the server:{e}")
        return None, None


global validate_data
def validate_data(system_name, source, df, column_name, condition_type, condition_value, check_type, primary_key, product_variant_column, error_type, indices_to_drop, total_records):
    """
    Validate Data and returns the filter df.
    """

    duplicates = pd.DataFrame()
    fails_df = pd.DataFrame()
    new_df = pd.DataFrame()

    if 'date' in column_name:
        if not pd.isna(condition_value):
            condition_value = condition_value.strftime('%Y-%m-%d')
            df[column_name] = pd.to_datetime(df[column_name], errors='coerce', dayfirst=True).dt.strftime('%Y-%m-%d')

    logging.warning(f"Validating {check_type}: {column_name} '{condition_type}'  '{condition_value}'")

    t_check_type = check_type.replace(" ", "_").lower()

    if t_check_type == "null_check":
        fails_df = df[(df[column_name].isna()) | (df[column_name] == '')]
        fail_indices = fails_df.index.tolist()

    elif t_check_type == 'unique_check':
        duplicates = df[df.duplicated(subset=[column_name], keep='first')]
        fail_indices = fails_df.index.tolist()
        
    # Validate condition_type/condition_value
    elif (condition_type is None and condition_value is not None) and (condition_type is None and condition_value is not None) :

        log_error(system_name, source, check_type, product_variant=None, error_message = "condition_type/condition_value is empty")
        logging.warning("condition_type/condition_value empty")

    # Validation 
    elif condition_type and condition_value:
        if condition_type == 'equal to':
            fails_df = df[df[column_name] == condition_value]
            fail_indices = fails_df.index.tolist()
        elif condition_type == 'not equal to':
            fails_df = df[df[column_name] != condition_value]            
            fail_indices = fails_df.index.tolist()          
        elif condition_type == 'greater than':
            fails_df = df[df[column_name] > condition_value]
            fail_indices = fails_df.index.tolist()
        elif condition_type == 'greater than equal to':
            fails_df = df[df[column_name] >= condition_value]
            fail_indices = fails_df.index.tolist()
        elif condition_type == 'less than':
            fails_df = df[df[column_name] < condition_value]
            fail_indices = fails_df.index.tolist()
        elif condition_type == 'less than equal to':
            fails_df = df[df[column_name] <= condition_value]
            fail_indices = fails_df.index.tolist()
        elif condition_type == "contains" or condition_type == "not contains":
            # condition_value is a master_name here
            all_conditions = list(master_repository.loc[master_repository['master_name'] == condition_value, 'source_column_value'])
            all_conditions_lower = [str(condition).lower() if isinstance(condition, str) else condition for condition in all_conditions]
            temp_series = df[column_name].apply(lambda x: x.lower() if isinstance(x, str) else x)

            if condition_type == "contains":
                fails_df = df[temp_series.isin(all_conditions_lower)]
                fail_indices = fails_df.index.tolist()
            else:
                fails_df = df[~(temp_series.isin(all_conditions_lower))]
                fail_indices = fails_df.index.tolist()
            condition_value = all_conditions
        else:
            logging.warning("Unknown type of condition_type")
            return
                
    else:

        log_error(system_name, source, check_type, product_variant=None, error_message = "condition_type and condition_value are empty")
        return

    # if fails log the error in report
    if not fails_df.empty:
        for i, row in fails_df.iterrows():
            position_id = row[primary_key]
            product_variant = row.get(product_variant_column, None)
            log_error(system_name, source, check_type, product_variant, position_id, column_name, f"{check_type} failed: {condition_type}: {condition_value}", error_type, total_records)
    
    # if duplicates found, log in report
    if not duplicates.empty:
        for i,row in duplicates.iterrows():
            position_id = row[primary_key]
            product_variant = row.get(product_variant_column, None)
            log_error(system_name, source, check_type, product_variant, position_id, column_name, f"{check_type} failed: {condition_type}: {condition_value}", error_type, total_records)
    
    logging.warning("Validated condition_type/condition_value")

    if error_type.upper() == "FATAL": # If FATAL drop, add fail_indices to indices_to_drop
        indices_to_drop.update(fail_indices)


global process_db_validation
def process_db_validations(all_db_validations):
    """
    Process all db validations.
    """
 
    # Iterate over each source
    for source, db_group in all_db_validations:
        logging.warning(f"Source_location: {source}")

        engine, connection, schema, inspector = get_db_connection(source)

        # if engine is None, skip
        if engine is None:
            logging.warning(f"Skipping Validation for source: {source}")
            continue 
        
        # if schema not found, skip
        if schema is None:
            logging.warning(f"Schema not found")
            logging.warning(f"Skipping Validation for source: {source}")
            continue
         
        table_group = db_group.groupby("source_table_name")

        # Iterate over each table
        for table, validations in table_group:
            
            table_name = table.lower()
            columns = inspector.get_columns(table_name)

            try:
                # Read table from db
                df = pd.read_sql_table(table_name, engine, schema=schema)
            except Exception as e:
                log_error(table, None, None, f"{e}")
                logging.warning(f"{e}")
                continue

            # Iterate over each validation in a each table in each source
            for validation in validations:

                column_name = validation['source_column_name'].lower()
                primary_key = validation['primary_key'].lower()
                expected_dtype = validation['condition_datatype'].lower()
                product_variant_column = validation.get('product_variant',"").lower()

                # condition to validate
                condition_type = validation['condition_type'].lower()
                condition_value = validation['condition_value']
                check_type = validation['check_type']
                error_type = validation['error_type']

                column_warning = next((column for column in columns if column['name'] == column_name), None)

                if column_warning is None:
                    log_error(table, None, None, f"Column does not exist: {column_name}")
                    logging.warning(f"Column does not exist: {column_name}")
                    continue
                

                # Get datatype from df, use it for db
                # try:
                #     df[column_name] = pd.to_datetime(df[column_name], errors='raise', dayfirst=True)
                #     actual_dtype = "datetime"
                # except ValueError:
                #     if pd.api.types.is_numeric_dtype(df[col]):
                #         actual_dtype = "numeric"
                #     else:
                #         actual_dtype = "character"

                actual_dtype = column_warning['type']
                
                df = validate_data(df, actual_dtype, expected_dtype, primary_key, condition_type, condition_value, check_type, product_variant_column, error_type)


global process_sftp_validations
def process_sftp_validations(all_sftp_validations):
    """
    Process all sftp validations.
    """
  
    # Iterate over each source
    for system_name, sftp_group in all_sftp_validations:
        logging.warning(f"Source_location: '{system_name}'")
        # sftp_client, path = get_sftp_client(system_name.strip())
        
        # if sftp_client is none, skip
        # if sftp_client is None:
        #     logging.warning(f"Skipped Validation for system_name: {system_name}")
        #     return 

        file_group = sftp_group.groupby("source_table_name")
        
        logging.warning("    ")
        logging.warning(f"length of file group: {file_group.ngroups}")

        # Iterate over each table
        for file, validations in file_group:
            
            global validation_error
            validation_error = {
                'extract_date': [], 'dq_run_date': [], 'system_name': [], 'stagingdb_name': [],
                'check_type': [], 'product_variant': [], 'position_id': [], 'dq_column_name': [],
                'error_message': [], 'error_type': [], 'total_records': [], 'total_records_after':[]
            }
            
            file_path = fr"{raw_data_dir}{file}"

            _, file_extension = os.path.splitext(file_path)

            logging.warning("    ")
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
                    log_error(system_name, file, error_message=f"Unknown file extension, skipped the {file}")
                    logging.warning(f"Unknown file extension, skipped the {file}")
                    continue
                
                global total_records
                total_records = len(df)
                logging.warning("File read successfully")
                logging.warning("   ")

            except FileNotFoundError as e:
                log_error(system_name, file, error_message=f"File not found: {e}, skipped the {file}")
                logging.warning(f"File not found: {e}, skipped the {file}")
                continue
            except Exception as e:
                log_error(system_name, file, error_message=f"File not found: {e}, skipped the {file}")
                logging.warning(f"Exception in sftp: {e}, skipped the {file}")
                continue

            logging.warning(f"No of rows to validate {len(validations)} in {file}")

            indices_to_drop = set() # For FATAL error

            # Iterate over each validation in a each table in each system_name
            for i,validation in validations.iterrows():
                logging.warning("   ")
                logging.warning(f"Validating row:{i+2}")
                logging.warning(f"df shape :{df.shape}")

                column_name = validation['source_column_name'].strip()
                expected_dtype = validation['condition_datatype'].lower().strip()
                primary_key = validation['primary_key'].strip()
                product_variant_column = validation.get('product_variant',"").strip() if isinstance(validation.get('product_variant',""), str) else validation.get('product_variant',"")
                target_table_name = validation['target_table_name'].strip()

                # condition to validate
                condition_type = validation['condition_type'].strip().lower() if isinstance(validation['condition_type'], str) else validation['condition_type']
                check_type = validation['check_type'].strip()
                condition_value = validation['condition_value']

                error_type = validation['error_type'].strip()
                if error_type.lower() == "fatal": # If FATAL drop 
                    drop = True
                elif error_type.lower() == "warning": # If WARNING, give warning
                    drop = False

                df.columns = df.columns.str.strip()
                
                if column_name not in df.columns:
                    
                    log_error(system_name, file, check_type, product_variant=None, position_id=None, dq_column_name=column_name, error_message=f"Column does not exist: {column_name}", error_type=error_type, total_records=total_records)
                    logging.warning(f"Column does not exist: {column_name}")
                    continue
                
                validate_data(system_name, file, df, column_name, condition_type, condition_value, check_type, primary_key, product_variant_column, error_type, indices_to_drop, total_records)

            if indices_to_drop:
                df = df.drop(list(indices_to_drop))

            all_error_free[target_table_name] = df
            
            # set length to end of all rows in dq_report
            new_df_length = len(df) # length after drop

            # Initialize length_list to the correct size with None values
            fail_count = validation_error['stagingdb_name'].count(file)
            length_list = [None] * fail_count
            validation_error['total_records_after'] += length_list

            length_list = np.array(validation_error['total_records_after'])
            indices = np.where(np.array(validation_error['stagingdb_name']) == file)[0]
            length_list[indices] = new_df_length
            validation_error['total_records_after'] = length_list.tolist()

            df_save_path = f"{output_path}{target_table_name}.xlsx"
            # error_file = fr"{dq_error_path}{target_table_name}_DQ_Report.xlsx"
            df_save_path_parquet = f"{parquet_path}{target_table_name}.parquet"

            try:
                # write df to local
                # df.to_excel(df_save_path, index = False)
                # logging.warning(f"Done for {file}, file:{df_save_path} saved")

                validation_error_df = pd.DataFrame(validation_error)

                # set length to end of all rows in dq_report
                length = len(df) # length after drop
                validation_error_df.loc[validation_error_df['stagingdb_name'] == file, 'total_records_after'] = length                

                # write df to parquet
                temp_table = pa.Table.from_pandas(df)
                pq.write_table(temp_table, df_save_path_parquet)

                # Write validation_error to sftp
                # excel_buffer = BytesIO()
                # validation_error_df.to_excel(excel_buffer, index=False)
                # excel_buffer.seek(0) 
                # sftp_client.putfo(excel_buffer, error_file)

                # logging.warning(f"Done for {file}, DQ report saved:{error_file}")

                # Adding content for source_column_list table
                source_column_list['source_name'].extend([system_name]*len(df.columns))
                source_column_list['source_table_name'].extend([file]*len(df.columns))
                source_column_list['source_table_column'].extend([col.replace('_', ' ').title() for col in df.columns])
                source_column_list['description'].extend([None]*len(df.columns))

            except Exception as e:
                
                logging.warning(f"{e}")
                log_error(system_name, file, error_message=f"{e}")
            
        logging.warning("   ")
        logging.warning("   ")
        logging.warning(f"Done for {system_name}")

global validate
def validate(all_checks):
    """
    Validate the checks.
    """
    
    # Drop rows where 'source_connection_type', 'source_table_name', 'source_column_name', or 'source_location' are empty
    all_checks = all_checks.copy()
    all_checks = all_checks.dropna(subset=['source_connection_type', 'source_table_name', 'source_column_name', 'source_location'])

    all_checks.loc[:, 'source_table_name'] = all_checks['source_table_name'].str.strip()
    all_checks.loc[:, 'source_location'] = all_checks['source_location'].str.strip()

    # Filter By source_connection_type, and group by source_location
    db_validations_to_perform = all_checks[all_checks['source_connection_type'].str.upper() == "DATABASE"].groupby('source_location')
    sftp_validations_to_perform = all_checks[all_checks['source_connection_type'].str.upper() == "SFTP"].groupby('source_location')
    
    if db_validations_to_perform.ngroups:
        logging.warning("Started Database Validation")
        process_db_validations(db_validations_to_perform)
        logging.warning("Completed Database Validation")
    
    logging.warning("    ")
  
    if sftp_validations_to_perform.ngroups:
        logging.warning("Started Sftp Validation")
        process_sftp_validations(sftp_validations_to_perform)
        logging.warning("Completed Sftp Validation")



# Main Code
global config_file_path, raw_data_dir
config_file_path = Antares_Master_File_Path
raw_data_dir = Raw_Data_Directory_Path

global dq_run_date
dq_run_date = datetime.now().strftime('%Y-%m-%d %H:%M')

# Ensure path ends with a slash
if not raw_data_dir.endswith('/'):
    raw_data_dir += '/'

global directory
directory = raw_data_dir + dq_run_date.replace(':', '-').replace(' ', '_') + "/"

global config_path, output_path, dq_error_path, parquet_path
config_path = fr"{directory}Config File/"
output_path = fr"{directory}Error Free/"
dq_error_path = fr"{output_path}DQ report/"
parquet_path = r"/opt/revolutio/Platform_Configs/alm_data/"

hostname = "20.244.30.89"
username = "azureuser"
user_secret_key = "pipeline@2024"
port = 22

# SFTP connection
global sftp_client
ssh_client = paramiko.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
logging.info("Connecting to the SFTP server...")
ssh_client.connect(hostname=hostname, username=username, password=user_secret_key, port=port)
sftp_client = ssh_client.open_sftp()
logging.info("Connected to the SFTP server")

# raise Exception("Connected to the SFTP server")

try:
    sftp_client.stat(directory)
except FileNotFoundError:
    # Create necessary folders
    for d in [directory, config_path, output_path, dq_error_path]:
        sftp_client.mkdir(d)

# Read Antares Master File
try:
    with sftp_client.file(config_file_path, 'rb') as remote_file:
        file_data = remote_file.read()
    master_file_buffer = BytesIO(file_data)
except FileNotFoundError:
    raise Exception(f"File not found:{config_file_path}")

global config_file, master_repository
config_file = pd.ExcelFile(master_file_buffer)
global_params = config_file.parse('global_parameter')

source_data_definition = Data1
source_data_preprocessing = Data2
master_repository = Data3

logging.warning(f"All config sheets are loaded")

# Saving Antares Master File for future reference
# sftp_client.putfo(master_file_buffer, os.path.join(config_path, 'Antares Data Framework Master.xlsx'))

# Read extract_date to log in DQ report
global extract_date
try:
    extract_date = global_params.loc[global_params['column_name'].str.lower().str.strip() == "reporting_date", 'value'].iloc[0].strftime("%Y-%m-%d")
except IndexError as e:
    extract_date = ""

global validation_error
validation_error = {
    'extract_date': [], 'dq_run_date': [], 'system_name': [], 'stagingdb_name': [],
    'check_type': [], 'product_variant': [], 'position_id': [], 'dq_column_name': [],
    'error_message': [], 'error_type': [], 'total_records': [], 'total_records_after':[]
}
logging.warning("    ")

global source_column_list
source_column_list = {
    'source_name':[],
    'source_table_name':[],
    'source_table_column':[],	
    'description':[]
}

# Processing source_data_preprocessing
global all_error_free
all_error_free = {}

logging.warning("Started processing source_data_definition")
validate(source_data_definition)
logging.warning("Ended processing source_data_definition")

logging.warning("Started processing source_data_preprocessing")
validate(source_data_preprocessing)
logging.warning("Ended processing source_data_preprocessing")

# save validation file
# l = [(i,len(v)) for i,v in validation_error.items()]
# raise Exception(f"{l}")
validation_error_df = pd.DataFrame(validation_error)

# error_file = fr"{dq_error_path}DQ Report.xlsx"
# excel_buffer = BytesIO()
# validation_error_df.to_excel(excel_buffer, index=False)
# excel_buffer.seek(0) 
# sftp_client.putfo(excel_buffer, error_file)

# save source_column_list table
source_column_list_df = pd.DataFrame(source_column_list)
source_column_list_df = source_column_list_df.astype(str)
logging.warning("   ")
logging.warning(f"Error logged in {dq_error_path}")

logging.warning(f"Error free files: {all_error_free.keys()}")
# Validation completed

output_data = validation_error_df