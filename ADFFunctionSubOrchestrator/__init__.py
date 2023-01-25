"""
    This "sub-orchestrator" function is not intended to be invoked directly. 
    Instead it will be invoked by the main orchestrator to manage the asynchronous 'fanning out' 
    of tasks to be completed by activity handlers.
"""

import logging
import json

import azure.functions as func
import azure.durable_functions as df
from ..shared_code.MyClasses import SerializableClass

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
    
def orchestrator_function(context: df.DurableOrchestrationContext):
    activity_pipeline_name = ""
    activity_pipeline_workload_name = ""

    error_message = ""
    results = {}
    results_list = []
    activity_functions_to_invoke_list = []

    total_activity_obj_found = 0
    total_activity_obj_processed = 0

    activity_functions_to_invoke = 0
    activity_functions_invoked = 0

    try:
        #activity_payload: SerializableClass = context.get_input()
        #activity_payload = context._input
        #activity_payload = json.loads(context.get_input())
        activity_input: str = context.get_input()
        """
        if (not isinstance(activity_input, dict)):
            error_message = f'Error trying to get sub_orchestrator input - expecting dictionary - input received: {activity_input}'
            log_message("EXCEPTION", error_message) 
            raise Exception(error_message)
        """
        activity_input = dict(activity_input)
        activity_payload = activity_input["activity_payload"]
        #activity_payload = json.loads(activity_input)
        #print(f'type(activity_payload): {type(activity_payload)}')
        #log_message("INFO", type(activity_payload))
        print(f'activity_payload: {activity_payload}')
        log_message("DEBUG", activity_payload)

        if (not activity_payload):
            error_message = f'Error trying to load sub_orchestrator activity_payload from input: {activity_payload}'
            log_message("EXCEPTION", error_message) 
            raise Exception(error_message)
        
        activity_pipeline_name = activity_payload.get('activity_pipeline_name',None)

        if (not activity_pipeline_name):
            error_message = f'Error trying to load sub_orchestrator activity_pipeline_name from input: {activity_payload}'
            log_message("EXCEPTION", error_message) 
            raise Exception(error_message)

        log_message("DEBUG", f'activity_pipeline_name{activity_pipeline_name}')

        activity_pipeline_workload_name = activity_payload.get('activity_pipeline_workload_name',None)

        if (not activity_pipeline_workload_name):
            error_message = f'Error trying to load sub_orchestrator activity_pipeline_workload_name from input: {activity_payload}'
            log_message("EXCEPTION", error_message) 
            raise Exception(error_message)

        log_message("DEBUG", f'activity_pipeline_workload_name{activity_pipeline_workload_name}')
        
        activity_pipeline_workload_purpose = activity_payload.get('activity_pipeline_workload_purpose',None)

        if (not activity_pipeline_workload_purpose):
            error_message = f'Error trying to load sub_orchestrator activity_pipeline_workload_purpose from input: {activity_payload}'
            log_message("EXCEPTION", error_message) 
            raise Exception(error_message)

        log_message("DEBUG", f'activity_pipeline_workload_purpose{activity_pipeline_workload_purpose}')
        
        activity_pipeline_resource = activity_payload.get('activity_pipeline_resource',None)

        if (not activity_pipeline_resource):
            error_message = f'Error trying to load sub_orchestrator activity_pipeline_resource from input: {activity_payload}'
            log_message("EXCEPTION", error_message) 
            raise Exception(error_message)

        log_message("DEBUG", f'activity_pipeline_resource{activity_pipeline_resource}')

        activity_task_list = activity_payload.get('activity_task_list',None)
        print(f'type(activity_task_list): {type(activity_task_list)}')
        print(f'activity_task_list: {activity_task_list}')

        if (not activity_task_list):
            error_message = f'Error trying to load orchestrator list from input: {activity_payload}'
            log_message("EXCEPTION", error_message) 
            raise Exception(error_message)

        if (not isinstance(activity_task_list, list)):
            error_message = f'Error - expected orchestrator list is not a list in input: {activity_task_list}'
            log_message("EXCEPTION", error_message) 
            raise Exception(error_message)
        
    except:
        error_message = f'Error trying to load input data from input: {context.get_input()}'
        log_message("EXCEPTION", error_message) 
        raise Exception(error_message) 
        
    print('Processing activity_task_list...')

    total_activity_obj_found += len(activity_task_list)

    if (isinstance(activity_task_list, list)):
        print(f'About to process activity_task_list: {activity_task_list}')
        total_activity_obj_processed += 1
        for activity_task_list_obj in activity_task_list:
            print(f'Processing activity_task_list_obj: {activity_task_list_obj}')
            print(f'type(activity_task_list_obj): {type(activity_task_list_obj)}')

            activity_functions_to_invoke += 1

            task_purpose = activity_task_list_obj.get('task_purpose',None)

            if (not task_purpose):
                error_message = f'Error trying to load task_purpose from activity_task_list_obj: {activity_task_list_obj}'
                log_message("EXCEPTION", error_message) 
                raise Exception(error_message)

            task_type = activity_task_list_obj.get('task_type',None)

            if (not task_type):
                error_message = f'Error trying to load task_type from activity_task_list_obj: {activity_task_list_obj}'
                log_message("EXCEPTION", error_message) 
                raise Exception(error_message)

            task_function_name = activity_task_list_obj.get('task_function_name',None)

            if (not task_function_name):
                error_message = f'Error trying to load task_function_name from activity_task_list_obj: {activity_task_list_obj}'
                log_message("EXCEPTION", error_message) 
                raise Exception(error_message)
            
            parameters = activity_task_list_obj["parameters"].get('iterations',None)

            if (not parameters):
                error_message = f'Error trying to load parameters from activity_task_list_obj: {activity_task_list_obj}'
                log_message("EXCEPTION", error_message) 
                raise Exception(error_message)

            activity_payload = {}
            activity_payload["activity_pipeline_name"] = activity_pipeline_name
            activity_payload["activity_pipeline_workload_name"] = activity_pipeline_workload_name
            activity_payload["activity_pipeline_workload_purpose"] = activity_pipeline_workload_purpose
            activity_payload["activity_pipeline_resource"] = activity_pipeline_resource
            activity_payload["task_purpose"] = task_purpose
            activity_payload["task_type"] = task_type
            activity_payload["task_function_name"] = task_function_name
            activity_payload["parameters"] = parameters

            activity_functions_to_invoke_list.append(context.call_activity("ADFFunction_qtmofssqlpg", activity_payload))
        
        if (len(activity_functions_to_invoke_list) > 0):
            results = yield context.task_all(activity_functions_to_invoke_list)
            activity_functions_invoked += len(activity_functions_to_invoke_list)
            results = f'activity_pipeline_name: {activity_pipeline_name} - activity_pipeline_workload_name: {activity_pipeline_workload_name} - activity_pipeline_workload_purpose: {activity_pipeline_workload_purpose} - Total functions invoked: {activity_functions_invoked} - activity_task_list: {activity_task_list} - Results: {results}'
            print (results)
            results_list.append(results)
            log_message("DEBUG", results)
        else:
            error_message = f'No tasks found for activity_task_list: {activity_task_list}'
            log_message("EXCEPTION", error_message) 
            raise Exception(error_message) 
    else:
        error_message = f'No activity_task_list provided in activity_payload: {json.dumps(activity_payload)}'
        log_message("EXCEPTION", error_message) 
        raise Exception(error_message) 

    log_message("DEBUG", f'Total total_activity_obj_found: {total_activity_obj_found}')
    log_message("DEBUG", f'Total total_activity_obj_processed: {total_activity_obj_processed}')
    log_message("DEBUG", f'Total activity_functions_to_invoke: {activity_functions_to_invoke}')
    log_message("DEBUG", f'Total activity_functions_invoked: {activity_functions_invoked}')
  
    return results_list

main = df.Orchestrator.create(orchestrator_function)
