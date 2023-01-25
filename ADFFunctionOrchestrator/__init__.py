"""
    This main "orchestrator" function is not intended to be invoked directly. Instead it will be triggered by an HTTP Start function.
    This function invokes sub-orchestrators to manage the asynchronous 'fanning out' of tasks to be completed by activity handlers.
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
    activity_task_list = []

    total_sub_orchestrator_obj_found = 0
    total_sub_orchestrator_obj_processed = 0

    total_activity_obj_found = 0
    total_activity_obj_processed = 0

    activity_functions_to_invoke = 0
    activity_functions_invoked = 0

    try:
        #orchestrator_payload: SerializableClass = context.get_input()
        #orchestrator_payload = context._input
        #orchestrator_payload = json.loads(context.get_input())
        orchestrator_payload: str = context.get_input()
        print(f'type(orchestrator_payload): {type(orchestrator_payload)}')
        print(f'orchestrator_payload: {orchestrator_payload}')

        if (not orchestrator_payload):
            error_message = f'Error trying to load orchestrator payload from input: {orchestrator_payload}'
            log_message("EXCEPTION", error_message) 
            raise Exception(error_message)
        
        orchestrator_list = orchestrator_payload.get('orchestrator',None)
        print(f'type(orchestrator_list): {type(orchestrator_list)}')
        print(f'orchestrator_list: {orchestrator_list}')

        if (not orchestrator_list):
            error_message = f'Error trying to load orchestrator list from input: {orchestrator_payload}'
            log_message("EXCEPTION", error_message) 
            raise Exception(error_message)

        if (not isinstance(orchestrator_list, list)):
            error_message = f'Error - expected orchestrator list is not a list in input: {orchestrator_list}'
            log_message("EXCEPTION", error_message) 
            raise Exception(error_message)
    except:
        error_message = f'Error trying to load orchestrator_list: {orchestrator_list}'
        log_message("EXCEPTION", error_message) 
        raise Exception(error_message) 
        
    print('Processing orchestrator_list...')

    total_sub_orchestrator_obj_found += len(orchestrator_list)

    for sub_orchestrator_obj in orchestrator_list:
        total_sub_orchestrator_obj_processed += 1
        sub_orchestrator_list = sub_orchestrator_obj.get('sub_orchestrator',None)
        activity_functions_to_invoke = 0
        activity_functions_invoked = 0
        if (isinstance(sub_orchestrator_list, list)):
            total_activity_obj_found += len(sub_orchestrator_list)
            activity_task_list_to_process = []
            for activity_obj in sub_orchestrator_list:
                total_activity_obj_processed += 1
                """
                activity_pipeline_name = activity_obj.get('activity_pipeline_name',None)
                activity_pipeline_workload_name = activity_obj.get('activity_pipeline_workload_name',None)
                activity_pipeline_workload_purpose = activity_obj.get('activity_pipeline_workload_purpose',None)
                activity_pipeline_resource = activity_obj.get('activity_pipeline_resource',None)
                activity_task_list = activity_obj.get('activity_task_list',None)
                payload = {}
                payload["activity_pipeline_name"] = activity_pipeline_name
                payload["activity_pipeline_workload_name"] = activity_pipeline_workload_name
                payload["activity_pipeline_workload_purpose"] = activity_pipeline_workload_purpose
                payload["activity_pipeline_resource"] = activity_pipeline_resource
                if (isinstance(activity_task_list, list)):
                    payload["activity_task_list"] = activity_task_list
                    activity_payload = {"activity_payload": payload }
                    activity_functions_to_invoke += len(activity_task_list)
                    task = f'activity_pipeline_name: {activity_pipeline_name} - activity_pipeline_workload_name: {activity_pipeline_workload_name} - activity_pipeline_workload_purpose: {activity_pipeline_workload_purpose} - Total functions to invoke: {activity_functions_to_invoke} - activity_task_list: {activity_task_list}'
                    print (task)
                    results_list.append(task)
                    activity_task_list_to_process.append(context.call_sub_orchestrator("ADFFunctionSubOrchestrator", activity_payload))
                    log_message("DEBUG", task)
                else:
                    error_message = f'No activity_task_list found for activity_task_list_obj: {json.dumps(activity_obj)}'
                    log_message("EXCEPTION", error_message) 
                    raise Exception(error_message) 
                if (len(activity_task_list_to_process) > 0): 
                    results = yield context.task_all(activity_task_list_to_process)
                    activity_functions_invoked += len(activity_task_list_to_process)
                    results = f'activity_pipeline_name: {activity_pipeline_name} - activity_pipeline_workload_name: {activity_pipeline_workload_name} - activity_pipeline_workload_purpose: {activity_pipeline_workload_purpose} - Total functions invoked: {activity_functions_invoked} - activity_task_list: {activity_task_list} - Results: {results}'
                    print (results)
                    results_list.append(results)
                    log_message("DEBUG", results)
                else:
                    error_message = f'No tasks found for activity_obj: {json.dumps(activity_obj)}'
                    log_message("EXCEPTION", error_message) 
                    raise Exception(error_message)
                """
            if (activity_functions_invoked == 0):
                error_message = f'No tasks invoked for activity_obj: {json.dumps(activity_obj)}'
                log_message("EXCEPTION", error_message) 
                raise Exception(error_message) 
            else:
                print(f'activity_pipeline_name: {activity_pipeline_name} activity_pipeline_workload_name: {activity_pipeline_workload_name}: Total functions to invoke {activity_functions_to_invoke} - Total functions invoked: {activity_functions_invoked}')
                log_message("DEBUG", f'Activity object sub-total total_sub_orchestrator_obj_found: {total_sub_orchestrator_obj_found}')
                log_message("DEBUG", f'Activity object sub-total total_sub_orchestrator_obj_processed: {total_sub_orchestrator_obj_processed}')
                log_message("DEBUG", f'Activity object sub-total total_activity_obj_found: {total_activity_obj_found}')
                log_message("DEBUG", f'Activity object sub-total total_activity_obj_processed: {total_activity_obj_processed}')
            
        else:
            error_message = f'No sub_orchestrator_list provided for sub_orchestrator_obj: {json.dumps(sub_orchestrator_obj)}'
            log_message("EXCEPTION", error_message) 
            raise Exception(error_message) 

        log_message("DEBUG", f'Sub-orchestrator sub-total total_sub_orchestrator_obj_found: {total_sub_orchestrator_obj_found}')
        log_message("DEBUG", f'Sub-orchestrator sub-total total_sub_orchestrator_obj_processed: {total_sub_orchestrator_obj_processed}')
        log_message("DEBUG", f'Sub-orchestrator sub-total total_activity_obj_found: {total_activity_obj_found}')
        log_message("DEBUG", f'Sub-orchestrator sub-total total_activity_obj_processed: {total_activity_obj_processed}')
        log_message("DEBUG", f'Sub-orchestrator sub-total activity_functions_to_invoke: {activity_functions_to_invoke}')
        log_message("DEBUG", f'Sub-orchestrator sub-total activity_functions_invoked: {activity_functions_invoked}')
    
    if (total_sub_orchestrator_obj_processed == 0):
        error_message = f'No sub_orchestrator_obj processed in orchestrator_list: {json.dumps(orchestrator_list)}'
        log_message("EXCEPTION", error_message) 
        raise Exception(error_message) 

    log_message("DEBUG", f'Total total_sub_orchestrator_obj_found: {total_sub_orchestrator_obj_found}')
    log_message("DEBUG", f'Total total_sub_orchestrator_obj_processed: {total_sub_orchestrator_obj_processed}')
    log_message("DEBUG", f'Total total_activity_obj_found: {total_activity_obj_found}')
    log_message("DEBUG", f'Total total_activity_obj_processed: {total_activity_obj_processed}')
    log_message("DEBUG", f'Total activity_functions_to_invoke: {activity_functions_to_invoke}')
    log_message("DEBUG", f'Total activity_functions_invoked: {activity_functions_invoked}')
 
    return results_list

main = df.Orchestrator.create(orchestrator_function)
