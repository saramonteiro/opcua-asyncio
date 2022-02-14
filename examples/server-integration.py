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

CONNECTION_STR = f'Endpoint=sb://eventhubnamespace-b4vijm.servicebus.windows.net/;SharedAccessKeyName=eventhub-key;SharedAccessKey=zqmYHPyI1BImoULo/EtMrdo4Y4HjX4SM2EsbeVo1bjc=;EntityPath=eventhub-b4vijm'

# Define callback to process event
def on_event(partition_context, event):
    global myvar
    telemetry = event.body_as_str()
    # print(telemetry)
    telemetry = json.loads(telemetry)
    print(telemetry["Value"])
    myvar.write_value(telemetry["Value"])

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    global myvar
    # Event Hub client
    client = EventHubConsumerClient.from_connection_string(conn_str=CONNECTION_STR, consumer_group="telemetrytsi")
    ConsumerThread = threading.Thread(
        target=client.receive,
        kwargs={
            "on_event": on_event
            # "starting_position": "-1",  # "-1" is from the beginning of the partition.
        }
    )
    _logger = logging.getLogger('asyncua')

    # setup our server
    server = Server()
    # server.init()
    server.set_endpoint('opc.tcp://0.0.0.0:4840/freeopcua/server/')
    
    # setup our own namespace, not really necessary but should as spec
    # uri = 'http://examples.freeopcua.github.io'
    uri = 'http://microsoft.com/Opc/OpcPlc'
    idx = server.register_namespace(uri)

    # populating our address space
    # server.nodes, contains links to very common nodes like objects and root
    myobj = server.nodes.objects.add_object(idx, 'MyObject')
    myvar = myobj.add_variable(idx, 'MyVariable', 0)
    # Set MyVariable to be writable by clients
    myvar.set_writable()
    _logger.info('Starting server!')  
    server.start()
    ConsumerThread.start()
    try:
        while True:
            time.sleep(1)
            new_val = myvar.get_value()
            _logger.info('Set value of %s to %d', myvar, new_val)
    finally:
        #close connection, remove subcsriptions, etc
        server.stop()
        client.close()