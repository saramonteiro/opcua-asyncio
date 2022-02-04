import logging
import asyncio
from syncer import sync
import sys
import time
from asyncua import ua, Server
from azure.eventhub import EventHubConsumerClient
import threading

# Outra ideia q tive: usar asyncio.to_thread para ter um loop (Vale a pena? n sei)

sys.path.insert(0, "..")

CONNECTION_STR = f'Endpoint=sb://iothub-ns-iiothub0-17157637-3182169def.servicebus.windows.net/;SharedAccessKeyName=service;SharedAccessKey=e54pv7WKFUDXBdh3DaEIjwsqfQp2ptALQnNNDXOIEYY=;EntityPath=iiothub0'

async def update_values(telemetry):
    global ft_vp, ft_min, ft_max, ft_ue, ft_tot, ft_rst, li_vp, li_ue
    await ft_vp.write_value(telemetry["FT-00-VP"])
    await ft_min.write_value(telemetry["FT-00-MIN"])
    await ft_max.write_value(telemetry["FT-00-MAX"])
    await ft_ue.write_value(telemetry["FT-00-UE"])
    await ft_tot.write_value(telemetry["FT-00-TOT"])
    await ft_rst.write_value(telemetry["FT-00-RST"])
    await li_vp.write_value(telemetry["LI-00-VP"])
    await li_ue.write_value(telemetry["LI-00-UE"])


# Define callback to process event
def on_event(partition_context, event):
    telemetry = event.body_as_json()
    print(telemetry)
    asyncio.run(update_values(telemetry))
    # asyncio.create_task(update_values(telemetry))

async def main():
    # logging.basicConfig(level=logging.DEBUG)
    global ft_vp, ft_min, ft_max, ft_ue, ft_tot, ft_rst, li_vp, li_ue
    # Event Hub client
    client = EventHubConsumerClient.from_connection_string(conn_str=CONNECTION_STR, consumer_group="$Default")
    # _logger = logging.getLogger('asyncua')
    # setup our server
    server = Server()
    await server.init()
    server.set_endpoint('opc.tcp://0.0.0.0:4840/freeopcua/server/')
    server.set_server_name("AEGEA - ETA 01 Server")
    # setup our own namespace, not really necessary but should as spec
    uri = 'http://microsoft.com/Opc/OpcPlc'
    idx = await server.register_namespace(uri)

    # populating our address space
    # server.nodes, contains links to very common nodes like objects and root
    ft = await server.nodes.objects.add_object(idx, 'FT-00')
    ft_vp = await ft.add_variable(idx, 'VP', 0)
    ft_min = await ft.add_variable(idx, 'MIN', 0)
    ft_max = await ft.add_variable(idx, 'MAX', 0)
    ft_ue = await ft.add_variable(idx, 'UE', "L/min")
    ft_tot = await ft.add_variable(idx, 'TOT', 0)
    ft_rst = await ft.add_variable(idx, 'RST', 0)
    li = await server.nodes.objects.add_object(idx, 'LI-00')
    li_vp = await li.add_variable(idx, 'VP', 0.0)
    li_ue = await li.add_variable(idx, 'UE', "LUX")
    # Set RST to be writable by clients
    await ft_rst.set_writable()
    # _logger.info('Starting server!')  
    await server.start()
    # task1 = asyncio.create_task(server.start())
    ConsumerThread = threading.Thread(target=client.receive, kwargs={"on_event": on_event})
    ConsumerThread.start()
    # task2 = asyncio.create_task(client.receive(on_event))

    try:
        while True:
            await asyncio.sleep(1)
    finally:
        #close connection, remove subcsriptions, etc
        server.stop()
        client.close()

if __name__ == '__main__':
    asyncio.run(main())
