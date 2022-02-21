import asyncio
import sys
sys.path.insert(0, "..")

from asyncua import  Server
from azure.eventhub.aio import EventHubConsumerClient

CONNECTION_STR = f'Endpoint=sb://iothub-ns-iothub-pr2-17437015-f9d8fd3d79.servicebus.windows.net/;SharedAccessKeyName=service;SharedAccessKey=ppdMVMTfPd1uPtfC/YwraezOxehJxGkjCoKg9arg1E8=;EntityPath=iothub-pr2qmy'

async def on_event(partition_context, event):
    global myvar
    telemetry = event.body_as_json()
    # Messages coming from IoT Edge are coming wrapped in list
    if type(telemetry) == list:
        #print("TELEMETRY", telemetry[0]["DisplayName"],telemetry[0]["Value"]["Value"])
        await myvar.write_value(telemetry[0]["Value"]["Value"])

async def main():
    global myvar

    print("Starting UA Server...")
    server = Server()

    await server.init()
    server.set_endpoint('opc.tcp://0.0.0.0:4840/aegea/eta02/server/')

    # setup our own namespace, not really necessary but should as spec
    uri = 'http://microsoft.com/Opc/OpcPlc'
    idx = await server.register_namespace(uri)

    # populating our address space
    # server.nodes, contains links to very common nodes like objects and root
    myobj = await server.nodes.objects.add_object(idx, 'Objects')
    myvar = await myobj.add_variable(idx, 'StepUp', 0)

    # Set MyVariable to be writable by clients
    await myvar.set_writable()

    await server.start()
    print('opc.tcp://0.0.0.0:4840/aegea/eta02/server/')

    print("Connecting to IoT Hub...")

    client = EventHubConsumerClient.from_connection_string(CONNECTION_STR, consumer_group="opcserver")
    
    print("Ready!")

    async with client:
        await client.receive(
            on_event=on_event
        )

if __name__ == '__main__':

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())