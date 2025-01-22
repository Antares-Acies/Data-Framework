global os
import os
import logging
import paramiko
import json
global create_engine, decrypt_existing_db_credentials, default_engine_creator
import pandas as pd
global inspect, create_engine, URL
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import URL
from Core.users.computations.db_credential_encrytion import decrypt_existing_db_credentials
from Core.users.computations.db_centralised_function import default_engine_creator 
global ssh_exception
from paramiko import ssh_exception
global PLATFORM_FILE_PATH, decrypt_existing_sftp_credentials
from config.settings.base import PLATFORM_FILE_PATH
from Core.users.computations.sftp_credential_encryption import decrypt_existing_sftp_credentials

global get_db_connection
def get_db_connection(database, location):

    logging.warning(f"Establishing {location} DB connection for: {database}")
    if location == "INTERNAL":
        db_creds_path = f'{PLATFORM_FILE_PATH}user_databases.json'
    else:
        db_creds_path = f'{PLATFORM_FILE_PATH}external_databases.json'

        
    # Connection to DB starts  
    try:
        with open(db_creds_path, 'r') as file:
            db_json = json.load(file)

        db_data = db_json[database]
        if db_data is None:
            # Terminate if DB not found in system
            logging.warning("Database creds not found in system. Terminating the execution")
            return None, None, None, None
            
        db_type = db_data["db_type"]
        if db_type not in ["MSSQL", "PostgreSQL", "Oracle"]:
            logging.warning("db type should be ['MSSQL', 'PostgreSQL', 'Oracle']")
            return None, None, None, None
    except Exception as e:
        logging.warning(f"Error in Db Connection:{e}")
        return None, None, None, None

    try:
        db_server, port, db_name, username, password = (
            decrypt_existing_db_credentials(
                db_data["server"],
                db_data["port"],
                db_data["db_name"],
                db_data["username"],
                db_data["password"],
                db_data["connection_code"],
            )
        )

        global schema
        schema = db_data.get("schema", None)
        db_data["server"] = db_server
        db_data["port"] = port
        db_data["db_name"] = db_name
        db_data["username"] = username
        db_data["password"] = password
        
        global engine, inspector, connection
        if db_type in ["MSSQL", "Oracle"]:
            engine = default_engine_creator(db_type=db_type, connection_details=db_data, return_pg_conn=False)
        else:
            connection_url = URL.create(
                "postgresql+psycopg2",
                username=db_data["username"],
                password=db_data["password"],
                host=db_data["server"],
                port=db_data["port"],
                database=db_data["db_name"]
            )
            engine = create_engine(connection_url)

        connection = engine.connect()
        inspector = inspect(engine)
        logging.warning(f"Successfully connected to Database: {db_data['db_name']}")
        return engine, connection, schema, inspector
    except Exception as e:
        logging.warning(f"Error in connecting to Database:{e}")
        return None, None, None, None


global get_sftp_connection
def get_sftp_connection(Sftp_Name):
    """
    Returns Sftp client connection object
    """

    logging.warning(f"Establishing connection for sftp: {Sftp_Name}")
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


global check_db
def check_db(all_db_sources, location):
    
    for connection_name, db_group in all_db_sources:
        
        engine, connection, schema, inspector = get_db_connection(connection_name, location)
        
        # if engine is None, skip
        if engine is None:
            logging.warning(f"Skipping Check for connection_name: {connection_name}")
            continue 
        
        tables = db_group["source_name"].unique()

        for table in tables:

            logging.warning(f"Avaliability Check for: {table}")

            report["Source Name"].append(table)
            report["Source Connection Type"].append("Database")
            report["Source Connection Location"].append(location)
            
            try:
                if inspector.has_table(table, schema=schema):
                    report["Exist"].append(True)
                else:
                    report["Exist"].append(False)
            except Exception as e:
                logging.warning(f"{table}: {e}")
                report["Exist"].append(False)
            
            logging.warning("   ")
        
        connection.close()
        engine.dispose()
        logging.warning(f"Done for {connection_name}")


global check_sftp
def check_sftp(all_sftp_sources):

    for connection_name, sftp_group in all_sftp_sources:
        
        sftp_client, ssh_client = get_sftp_connection(connection_name.strip())

        # if sftp_client is none, skip
        if sftp_client is None:
            logging.warning(f"Skipped Filtering for connection_name: {connection_name}")
            return 

        files = sftp_group["source_name"].unique()
        # Iterate over each file
        for file in files:

            logging.warning(f"Avaliability Check for: {file}")
            report["Source Name"].append(file)
            report["Source Connection Type"].append("SFTP")

            try:
                path = sftp_group.loc[sftp_group['source_name'] == file, 'source_connection_location'].iloc[0]
                report["Source Connection Location"].append(path)
                file_path = os.path.join(path,file)
                if sftp_client.stat(file_path):
                    report["Exist"].append(True)
                else:
                    report["Exist"].append(False)
            except Exception as e:
                logging.warning(f"{file}: {e}")
                report["Exist"].append(False)

            logging.warning("   ")
        
        ssh_client.close()
        sftp_client.close()
        logging.warning(f"Done for {connection_name}")


global check
def check(source_definitions):
    
    try:
        # Filter By source_connection_type, and group by source_location
        internal_db_check_to_perform = source_definitions[
            (source_definitions['source_connection_type'].astype(str).str.upper() == "DATABASE") &
            (source_definitions['source_connection_location'].astype(str).str.upper() == "INTERNAL")
        ].groupby('connection_name')
        external_db_check_to_perform = source_definitions[
            (source_definitions['source_connection_type'].astype(str).str.upper() == "DATABASE") & 
            (source_definitions['source_connection_location'].astype(str).str.upper() == "EXTERNAL")
        ].groupby('connection_name')
        sftp_check_to_perform = source_definitions[source_definitions['source_connection_type'].astype(str).str.upper() == "SFTP"].groupby('connection_name')
    except Exception as e:
        logging.warning("Error while groupby and filtering with db/sftp and location")
        return
        
    if internal_db_check_to_perform.ngroups:
        logging.warning("Started Internal Database Validation")
        check_db(internal_db_check_to_perform, 'INTERNAL')
        logging.warning("Completed Internal Database Validation")
        logging.warning("   ")
        logging.warning("   ")

    if external_db_check_to_perform.ngroups:
        logging.warning("Started External Database Validation")
        check_db(external_db_check_to_perform, 'EXTERNAL')
        logging.warning("Completed External Database Validation")    
        logging.warning("   ")
        logging.warning("   ")

    if sftp_check_to_perform.ngroups:
        logging.warning("Started Sftp Validation")
        check_sftp(sftp_check_to_perform)
        logging.warning("Completed Sftp Validation")
        logging.warning("   ")
        logging.warning("   ")
        

global report
report = {'Source Name': [], 'Source Connection Type': [], 'Source Connection Location':[], 'Exist': []}

source_definitions = Data1
check(source_definitions)

logging.warning("Source Availability Check Completed")

output_data = pd.DataFrame(report)
output_data = output_data.astype(str)
output_data["id"] = output_data.index + 1
output_data.insert(0, 'id', output_data.pop('id'))