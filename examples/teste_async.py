import asyncio
import time

async def say_after(delay, what):
    await asyncio.sleep(delay)
    print(what)

def teste():
    task1 = asyncio.create_task(
        say_after(1, 'hello'))

async def main():

    teste()
    task2 = asyncio.create_task(
        say_after(2, 'world'))

    print(f"started at {time.strftime('%X')}")

    # Wait until both tasks are completed (should take
    # around 2 seconds.)
    # await task1
    # await task2
    while True:
        await asyncio.sleep(1)

    print(f"finished at {time.strftime('%X')}")

# def main():
#     task1 = asyncio.create_task(
#         say_after(1, 'hello'))
#     while True:
#         time.sleep(1)


asyncio.run(main())
