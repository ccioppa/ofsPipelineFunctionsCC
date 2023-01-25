""" This function an HTTP starter function for Durable Functions, which calls an 
    orchestrator function that manages the 'fanning out' of work to sub-orchestrator that 
    asynchronously starts tasks to be completed by activity handlers.
"""
 
import logging

import azure.functions as func
import azure.durable_functions as df
#from ..shared_code.MyClasses import SerializableClass
import json

async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)
    #orchestrator_payload = req.get_body()
    orchestrator_payload = json.loads(req.get_body().decode('utf-8'))
    #request_payload = req.get_payload()
    print(f'type(orchestrator_payload): {type(orchestrator_payload)}')
    logging.info(f'orchestrator_payload: {orchestrator_payload}')
    function_name = req.route_params['functionName']

    logging.info = (f'About to start ADFFunctionOrchestrator - Event Payload: {orchestrator_payload}')
    instance_id = await client.start_new(function_name, None, orchestrator_payload)
    logging.info = (f'ADFFunctionOrchestrator started with Instance ID : {instance_id}')

    return client.create_check_status_response(req, instance_id)