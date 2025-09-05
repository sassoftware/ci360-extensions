# Background refresh token after expiration


import asyncio

def start_background_task(coro):
    loop = asyncio.get_event_loop()
    loop.create_task(coro)
