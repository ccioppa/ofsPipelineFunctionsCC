# This function is a 'handler' to execute task initiated by a calling orchestrator function.  
# It is not intended to be invoked directly.
import os
import logging
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO,StringIO
import ssl
from urllib.parse import quote_plus
from decouple import config
import datetime
import sys
import json
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
"""
PostgreSQL Function calls post ADF completion:
Bi-Weekly Pipeline 
   process_l2r_lte_congestion
   process_l2r_nr_congestion
   process_l2r_magenta_build
   process_r2s_magenta_build
   process_agg_sector
   process_agg_rec

Monthly Pipeline
   process_l2r_hsi_sector
   process_l2r_smra
   process_r2s_building
   process_map_bldg_ookla
   process_agg_rec

run_function_once('dev.process_l2r_building', 1, engine)
run_function_once('dev.process_l2r_lte_congestion' 1, engine)
run_function_once('dev.process_l2r_nr_congestion', 1, engine)
run_function_once('dev.process_l2r_dnb_combined', 1, engine)
run_function_once('dev.process_l2r_engg_market_boundary', 1, engine)

run_function_once('dev.process_l2r_hsi_sector', 7, engine) # check

run_function_once('dev.process_l2r_magenta_build', 1, engine)
run_function_once('dev.process_l2r_ookla', 1, engine) # check
run_function_once('dev.process_l2r_smra_boundary', 1, engine)
run_function_once('dev.process_l2r_smra', 1, engine)
run_function_once('dev.process_l2r_starling_lte', 1, engine)
run_function_once('dev.process_l2r_starling_nr', 1, engine)
run_function_once('dev.process_r2s_magenta_build', 1, engine)
run_function_once('dev.process_r2s_ookla', 1, engine) # check

run_function_once('dev.process_agg_dnb', 1, engine)
run_function_once('dev.process_map_building', 1, engine)
run_function_once('dev.process_map_bldg_site', 1, engine)

run_function_iterations('dev.process_r2s_building', 50, engine)

run_function_once('dev.process_agg_sector', 1, engine)
"""
seq = log_df = engine = None
results = []

def log_message(sev, msg):
    if (sev == "INFO"):
        log_message("DEBUG", msg)
    elif (sev == "WARNING"):
        logging.warning(msg)
    elif (sev == "ERROR"):
        logging.error(msg) 
    elif (sev == "CRITICAL"):
        logging.critical(msg) 
    elif (sev == "LOG"):
        logging.log(msg)            
    elif (sev == "EXCEPTION"):
        logging.exception(msg) 
    return
    """
    activity_payload["activity_pipeline_name"] = activity_pipeline_name
    activity_payload["activity_pipeline_workload_name"] = activity_pipeline_workload_name
    activity_payload["activity_pipeline_workload_purpose"] = activity_pipeline_workload_purpose
    activity_payload["task_purpose"] = task_purpose
    activity_payload["task_type"] = task_type
    activity_payload["task_function_name"] = task_function_name
    activity_payload["parameters"] = parameters
    """
def main(task: str) -> str:
    global seq, log_df, engine
    print(f'type(task): {type(task)}')
    print(f'task: {task}')
    log_message("DEBUG", f'task{task}')
    host = task['activity_pipeline_resource']['host']
    database = task['activity_pipeline_resource']['database']
    username = task['activity_pipeline_resource']['username']
    port = task['activity_pipeline_resource']['port']

    if (task['activity_pipeline_resource']['authentication_type'] == "secret"):
        identity = DefaultAzureCredential()
        secretClient = SecretClient(vault_url="https://ofskv.vault.azure.net/", credential=identity)
        secret = secretClient.get_secret(task['activity_pipeline_resource']['password_secret'])
        password = secret.value
    else:
        password = task['activity_pipeline_resource']['password_secret']
    
    log_message("DEBUG", f'username{username}')
    log_message("DEBUG", f'password{password}')
    log_message("DEBUG", f'host{host}')
    log_message("DEBUG", f'port{port}')
    log_message("DEBUG", f'database{database}')

    engine = ("postgresql://" + username + ":{0}@" + host + ":" + port + "/" + database).format(quote_plus(password))
    
    seq = 0
    ct = datetime.datetime.now()
    log_init_data = [[seq, 'default', ct]]
    log_df = pd.DataFrame(log_init_data, columns=['Sequence', 'Function', 'Timestamp'])
    if (task['task_type'] == "run_function_once"):
        run_function_once(task['task_function_name'], 1, engine)
    elif (task['task_type'] == "run_function_iterations"):
        run_function_iterations(task['task_function_name'], int(task['parameters']['iterations']), engine)
    else:
        error_message = f'Error - Invalid task type in payload: {task}'
        log_message("EXCEPTION", error_message) 
        raise Exception(error_message)

    results = log_df.to_json()
    log_message("DEBUG", f'Results: {results}')
    return [results]

def log_time(this_seq, this_func):
    global log_df
    new_row = pd.Series({'Sequence': this_seq, 'Function': this_func, 'Timestamp': datetime.datetime.now()})
    log_df = pd.concat([log_df, new_row.to_frame().T], ignore_index=True)
    return

def check_exit(func, cursor):
    try:
        sql_data = (func, )
        sql = "SELECT dev.check_graceful_exit(%s);"
        cursor.execute(sql, sql_data)
        exit_ind = cursor.fetchone()[0]
    except Exception as e:
        log_message("DEBUG", e)
        cursor.close()
        sys.exit("Cannot retrieve graceful exit")
    return exit_ind

def run_function_iterations(in_func, iterations, eng):
    global seq, log_df
    log_message("DEBUG", 'Begin ' + in_func)
    eng = create_engine(engine, isolation_level="AUTOCOMMIT")
    connection = eng.raw_connection()
    cursor = connection.cursor()
    log_message("DEBUG", 'Connection opened.')
    try:
        for x in range(iterations):
            if check_exit('default', cursor) == 'Y':
                log_message("DEBUG", 'Gracefully exiting...')
                break
            x = x + 1
            log_message("DEBUG", x)
            run_data = (in_func, x)
            run_query = "CALL dev.run_function(%s, %s);"
            seq = seq + 1
            log_time(seq, in_func+':'+str(iterations))
            cursor.execute(run_query, run_data)
            connection.commit()
            log_time(seq, in_func+':'+str(iterations))
            log_message("DEBUG", 'Commit successful.')
    except Exception as e:
        log_message("DEBUG", e)
        connection.rollback()
        log_message("DEBUG", 'Rollback successful.')
    cursor.close()
    log_message("DEBUG", 'Connection closed.')
    log_message("DEBUG", 'End '+ in_func)
    return

def run_function_once(in_func, iterations, eng):
    global seq, log_df
    log_message("DEBUG", 'Begin ' + in_func)
    eng = create_engine(engine, isolation_level="AUTOCOMMIT")
    connection = eng.raw_connection()
    cursor = connection.cursor()
    log_message("DEBUG", 'Connection opened.')
    if check_exit('default', cursor) == 'Y':
        log_message("DEBUG", 'Gracefully exiting...')
        cursor.close()
        log_message("DEBUG", 'Connection closed.')
        log_message("DEBUG", 'End '+ in_func)
        return
    try:
        run_data = (in_func, iterations)
        run_query = "CALL dev.run_function(%s, %s);"
        seq = seq + 1
        log_time(seq, in_func+':'+str(iterations))
        cursor.execute(run_query, run_data)
        log_time(seq, in_func+':'+str(iterations))
        connection.commit()
        log_message("DEBUG", 'Commit successful.')
    except Exception as e:
        log_message("DEBUG", e)
        connection.rollback()
        log_message("DEBUG", 'Rollback successful.')
    cursor.close()
    log_message("DEBUG", 'Connection closed.')
    log_message("DEBUG", 'End '+ in_func)
    return