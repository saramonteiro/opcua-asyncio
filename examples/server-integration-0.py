import logging
import asyncio
import sys
import time
from asyncua.common import node_factory
from asyncua.common.callback import CallbackType

sys.path.insert(0, "..")

# from asyncua import ua, Server
from asyncua.sync import Server
# from asyncua.common.methods import uamethod
from azure.eventhub import EventHubConsumerClient
import threading
import json

# CONNECTION_STR = f'Endpoint=sb://ihsuprodblres079dednamespace.servicebus.windows.net/;SharedAccessKeyName=service;SharedAccessKey=qquErAz0neKKCEpd3agXzhZ1oiPTDu/G+RlAvC/lZzo=;EntityPath=iothub-ehub-sarahub-16710098-e06deed358'
CONNECTION_STR = f'Endpoint=sb://iothub-ns-iiothub0-17157637-3182169def.servicebus.windows.net/;SharedAccessKeyName=service;SharedAccessKey=e54pv7WKFUDXBdh3DaEIjwsqfQp2ptALQnNNDXOIEYY=;EntityPath=iiothub0'

# Define callback to process event
def on_event(partition_context, event):
    global myvar
    global ft_vp, ft_min, ft_max, ft_ue, ft_tot, ft_rst, li_vp, li_ue
    # telemetry = event.body_as_str()
    telemetry = event.body_as_json()
    print(telemetry)
    # telemetry = json.loads(telemetry)
    # print(telemetry)
    # print(telemetry["DisplayName"], telemetry["Value"])
    # myvar.write_value(telemetry["Value"])
    ft_vp.write_value(telemetry["FT-00-VP"])
    ft_min.write_value(telemetry["FT-00-MIN"])
    ft_max.write_value(telemetry["FT-00-MAX"])
    ft_ue.write_value(telemetry["FT-00-UE"])
    ft_tot.write_value(telemetry["FT-00-TOT"])
    ft_rst.write_value(telemetry["FT-00-RST"])
    li_vp.write_value(telemetry["LI-00-VP"])
    li_ue.write_value(telemetry["LI-00-UE"])


# def create_monitored_items(event, dispatcher):
#     print("Monitored Item")

#     for idx in range(len(event.response_params)):
#         if event.response_params[idx].StatusCode.is_good():
#             nodeId = event.request_params.ItemsToCreate[idx].ItemToMonitor.NodeId
#             print(f"Node {nodeId} was created")


# def modify_monitored_items(event, dispatcher):
#     print("modify_monitored_items")


# def delete_monitored_items(event, dispatcher):
#     print("delete_monitored_items")

if __name__ == '__main__':
    # logging.basicConfig(level=logging.DEBUG)
    global myvar
    global ft_vp, ft_min, ft_max, ft_ue, ft_tot, ft_rst, li_vp, li_ue
    # Event Hub client
    client = EventHubConsumerClient.from_connection_string(conn_str=CONNECTION_STR, consumer_group="$Default")
    ConsumerThread = threading.Thread(
        target=client.receive,
        kwargs={
            "on_event": on_event
            # "starting_position": "-1",  # "-1" is from the beginning of the partition.
        }
    )
    # _logger = logging.getLogger('asyncua')

    # setup our server
    server = Server()
    # server.init()
    server.set_endpoint('opc.tcp://0.0.0.0:4840/freeopcua/server/')
    server.set_server_name("AEGEA - ETA 01 Server")
    
    # setup our own namespace, not really necessary but should as spec
    # uri = 'http://examples.freeopcua.github.io'
    uri = 'http://microsoft.com/Opc/OpcPlc'
    idx = server.register_namespace(uri)

    # populating our address space
    # server.nodes, contains links to very common nodes like objects and root
    ft = server.nodes.objects.add_object(idx, 'FT-00')
    ft_vp = ft.add_variable(idx, 'VP', 0)
    ft_min = ft.add_variable(idx, 'MIN', 0)
    ft_max = ft.add_variable(idx, 'MAX', 0)
    ft_ue = ft.add_variable(idx, 'UE', "L/min")
    ft_tot = ft.add_variable(idx, 'TOT', 0)
    ft_rst = ft.add_variable(idx, 'RST', 0)
    li = server.nodes.objects.add_object(idx, 'LI-00')
    li_vp = li.add_variable(idx, 'VP', 0.0)
    li_ue = li.add_variable(idx, 'UE', "LUX")
    # Set MyVariable to be writable by clients
    ft_rst.set_writable()
    # _logger.info('Starting server!')  
    server.start()
    # Create Callback for item event
    # server.subscribe_server_callback(CallbackType.ItemSubscriptionCreated, create_monitored_items)
    # server.subscribe_server_callback(CallbackType.ItemSubscriptionModified, modify_monitored_items)
    # server.subscribe_server_callback(CallbackType.ItemSubscriptionDeleted, delete_monitored_items)
    ConsumerThread.start()
    var = 10
    try:
        while True:
            time.sleep(1)
            # ft_vp.write_value(var)
            # var = var+10
            print("teste")
            # new_val = myvar.get_value()
            # _logger.info('Set value of %s to %d', myvar, new_val)
    finally:
        #close connection, remove subcsriptions, etc
        server.stop()
        client.close()