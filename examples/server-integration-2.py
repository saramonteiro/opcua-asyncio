from email import message
import logging
import asyncio
from syncer import sync
import sys
import time
from asyncua import ua, Server, Node
from azure.eventhub import EventHubConsumerClient
import threading
from azure.iot.hub import IoTHubRegistryManager
from threading import Timer

# Outra ideia q tive: usar asyncio.to_thread para ter um loop (Vale a pena? n sei)

sys.path.insert(0, "..")

CONNECTION_STR = f'Endpoint=sb://iothub-ns-jefter-iot-17261576-9d69730fe6.servicebus.windows.net/;SharedAccessKeyName=service;SharedAccessKey=N7AZH3v7rTXrBupV/CRWG8G62rDh9+qsCFxG49nayPU=;EntityPath=jefter-iothub'
connection_str = 'HostName=Jefter-IoThub.azure-devices.net;SharedAccessKeyName=service;SharedAccessKey=N7AZH3v7rTXrBupV/CRWG8G62rDh9+qsCFxG49nayPU='
device_id = 'Device0'
beacon_interval = 60
global last_ft_rst

class SubscriptionHandler:
    """
    The SubscriptionHandler is used to handle the data that is received for the subscription.
    """
    def __init__(self):
        # Create IoTHubRegistryManager
        self.registry_manager = IoTHubRegistryManager.from_connection_string(connection_str)
        self.registerTimeCallback(beacon_interval, self.keepConnectionAlive,
                                  [beacon_interval,self.registry_manager])

    def keepConnectionAlive(self, interval, manager):
        manager.send_c2d_message(device_id, "beacon")
        print("Beacon Enviado")
        Timer(interval, self.keepConnectionAlive, [interval, manager]).start()

    def registerTimeCallback(self, interval, callback, args):
        Timer(interval, callback, args).start()

    async def datachange_notification(self, node: Node, val, data):
        """
        Callback for asyncua Subscription.
        This method will be called when the Client received a data change message from the Server.
        """
        name = await node.read_display_name()
        name = name.Text
        if name == 'RST':
            message = '{{"FT-00-RST": {0}}}'.format(val)
        elif name == 'STATUS':
            message = '{{"LS-00-STATUS": {0}}}'.format(val)
        print('datachange_notification ', name, val)      
        self.registry_manager.send_c2d_message(device_id, message)

# Callback triggered when telemetry arrives.
async def update_values(telemetry):
    global ft_vp, ft_min, ft_max, ft_ue, ft_tot, ft_rst, li_vp, li_ue
    global last_ft_rst # Last rst value on device
    global first_time
    await ft_vp.write_value(telemetry["FT-00-VP"])
    await ft_min.write_value(telemetry["FT-00-MIN"])
    await ft_max.write_value(telemetry["FT-00-MAX"])
    await ft_ue.write_value(telemetry["FT-00-UE"])
    await ft_tot.write_value(telemetry["FT-00-TOT"])
    # Define the variable on the 1st time
    if first_time == True:
        last_ft_rst = telemetry["FT-00-RST"]
        first_time = False
    # We can only change the reset value when the previous value
    # is consistent with the current value on the OPC UA server.
    rst = await ft_rst.get_value() 
    print(last_ft_rst, rst)
    # Wasn't the change propagated yet or is it the device saying the totalizer was zeroed?
    if rst != telemetry["FT-00-RST"]: 
        # The change was propagated, it's the device trying to update the value.  
        if last_ft_rst == rst:
            await ft_rst.write_value(telemetry["FT-00-RST"])
            last_ft_rst = telemetry["FT-00-RST"]
        # Change was not propagated yet. Don't update the variable with an outdated state.
        else:
            pass
    # Consistent state, update the last rst value
    else:
        await ft_rst.write_value(telemetry["FT-00-RST"]) # not really necessary
        # Update last rst value
        if telemetry["FT-00-RST"] != last_ft_rst:
            last_ft_rst = telemetry["FT-00-RST"]
    print(last_ft_rst, rst)
    status = await ls_st.get_value()
    # Wasn't the change propagated yet?
    if status != telemetry["LS-00-STATUS"]: 
    # Don't update the variable with an outdated state. Only OPC UA client updates the variable. 
        pass
    else:
        await ls_st.write_value(telemetry["LS-00-STATUS"]) # not really necessary
    await li_vp.write_value(telemetry["LI-00-VP"])
    await li_ue.write_value(telemetry["LI-00-UE"])


# Define callback to process event
def on_event(partition_context, event):
    # print(event)
    telemetry = event.body_as_json()
    print(telemetry)
    asyncio.run(update_values(telemetry))


async def main():
    # logging.basicConfig(level=logging.DEBUG)
    global ft_vp, ft_min, ft_max, ft_ue, ft_tot, ft_rst, li_vp, li_ue, ls, ls_st
    global first_time
    first_time = True
    # Event Hub client
    client = EventHubConsumerClient.from_connection_string(conn_str=CONNECTION_STR, consumer_group="$Default")
    # _logger = logging.getLogger('asyncua')
    # setup our server
    server = Server()
    await server.init()
    server.set_endpoint('opc.tcp://0.0.0.0:4840/aegea/eta01/server/') # opc.tcp://localhost:4840/aegea/eta01/server/
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
    # Set RST to be writable by clients
    await ft_rst.set_writable()
    li = await server.nodes.objects.add_object(idx, 'LI-00')
    li_vp = await li.add_variable(idx, 'VP', 0.0)
    li_ue = await li.add_variable(idx, 'UE', "LUX")
    ls = await server.nodes.objects.add_object(idx, 'LS-00')
    ls_st = await ls.add_variable(idx, 'STATUS', 0)
    await ls_st.set_writable()
    # _logger.info('Starting server!')  
    await server.start()
    handler = SubscriptionHandler()
    subscription = await server.create_subscription(500, handler)
    await subscription.subscribe_data_change(ft_rst)
    await subscription.subscribe_data_change(ls_st)
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
