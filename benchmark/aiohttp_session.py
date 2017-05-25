import asyncio
import time

import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

import aiohttp


conn = aiohttp.TCPConnector(
    use_dns_cache=True,
    verify_ssl=False,
    keepalive_timeout=0,
)
session = aiohttp.ClientSession(connector=conn)




async def request():
    # url = 'http://127.0.0.1:8080/index.html'
    url = 'http://127.0.0.1:8080/taobao.html'
    async with session.request('GET', url) as resp:
        await resp.read()


async def test():
    start = time.time()
    print('start: ', start)
    await asyncio.gather(*(
        request() for i in range(1000)
    ))
    end = time.time()
    print('end:', end)
    print('cost:', end - start)



def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test())
    session.close()


if __name__ == '__main__':
    main()
