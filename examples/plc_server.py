import logging
import asyncio
import sys
import time
from asyncua.common import node_factory
sys.path.insert(0, "..")

# from asyncua import ua, Server
from asyncua.sync import Server
# from asyncua.common.methods import uamethod
from azure.eventhub import EventHubConsumerClient
import threading
import json

CONNECTION_STR = f'Endpoint=sb://iothub-ns-iothub-pr2-17437015-f9d8fd3d79.servicebus.windows.net/;SharedAccessKeyName=service;SharedAccessKey=ppdMVMTfPd1uPtfC/YwraezOxehJxGkjCoKg9arg1E8=;EntityPath=iothub-pr2qmy'
# Define callback to process event
def on_event(partition_context, event):
    global myvar
    telemetry = event.body_as_json()
    # Messages coming from IoT Edge are coming wrapped in list
    if type(telemetry) == list:
        print(telemetry[0]["DisplayName"], telemetry[0]["Value"]["Value"])
        myvar.write_value(telemetry[0]["Value"]["Value"])

if __name__ == '__main__':
    # logging.basicConfig(level=logging.DEBUG)
    global myvar
    # Event Hub client
    client = EventHubConsumerClient.from_connection_string(conn_str=CONNECTION_STR, consumer_group="opcserver")
    ConsumerThread = threading.Thread(
        target=client.receive,
        kwargs={"on_event": on_event}
    )
    # setup our server
    server = Server()
    # server.init()
    server.set_endpoint('opc.tcp://0.0.0.0:4840/aegea/eta02/server/')
    
    # setup our own namespace, not really necessary but should as spec
    uri ='http://microsoft.com/Opc/OpcPlc'
    idx = server.register_namespace(uri)

    # populating our address space
    # server.nodes, contains links to very common nodes like objects and root
    myobj = server.nodes.objects.add_object(idx, 'Objects')
    # Unsigned int from simulated PLC
    myvar = myobj.add_variable(idx, 'StepUp', 0) 
    # Set MyVariable to be writable by clients
    myvar.set_writable()
    # _logger.info('Starting server!')  
    server.start()
    ConsumerThread.start()
    try:
        while True:
            time.sleep(1)
    finally:
        #close connection, remove subcsriptions, etc
        server.stop()
        client.close()