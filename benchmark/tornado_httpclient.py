import asyncio
import time

from tornado import gen, ioloop
from tornado.httpclient import AsyncHTTPClient



@gen.coroutine
def request():
    http_client = AsyncHTTPClient()
    # url = 'http://127.0.0.1:8080/index.html'
    url = 'http://127.0.0.1:8080/taobao.html'
    yield http_client.fetch(url)


@gen.coroutine
def test():
    start = time.time()
    print('start: ', start)
    yield [
        request() for i in range(1000)
    ]
    end = time.time()
    print('end:', end)
    print('cost:', end - start)



def main():
    io_loop = ioloop.IOLoop.current()
    io_loop.run_sync(test)


if __name__ == '__main__':
    main()

