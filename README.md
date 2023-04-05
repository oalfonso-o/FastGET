# Patata

<p align="center">
    <em>Easy parallel and concurrent requests</em>
</p>

<p align="center">
<a href="https://github.com/oalfonso-o/patata/actions?query=workflow%3ACI+event%3Apush+branch%3Amain" target="_blank">
    <img src="https://github.com/oalfonso-o/patata/workflows/CI/badge.svg?event=push&branch=main" alt="Test">
</a>
<a href="https://pypi.org/project/patata" target="_blank">
    <img src="https://img.shields.io/pypi/v/patata?color=%2334D058&label=pypi%20package" alt="Package version">
</a>
<a href="https://pypi.org/project/patata" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/patata.svg?color=%2334D058" alt="Supported Python versions">
</a>
</p>

The idea of this package is to wrap `multiprocessing` and `async` concurrency and allow the user to perform thousands of requests in parallel and concurrently without having to worry about pools of processes and async event loops.

It only supports GET and POST requests. More methods will be implemented in later versions.

The input is an iterable and the output is a generator. As soon as the requests get their response they are yielded until all the requests of the input are done.

Each element of the iterable is a patata.Request object with `id_`, `url` and `data`.
Each element yielded is a patata.Response with the same `id_`, `status_code` and `data` which is the json response. With the `id_` you can map the responses to the input requests.

This is useful for cases where you have a huge amount of requests to perform to an API and you
need to do them as fast as possible.

The client by default detects the number of CPUs available and starts one process per each CPU.
Then chunks the input iterator to provide requests to all the processes.
Each process for each task opens an event loop and performs all those requests concurrently. Once
all requests are awaited, the chunk with all the responses is returned back to the main process.
This is why we can see that our generator is receiving the responses un bulks.

## Install:

From PyPi:
```
pip install patata
```

## Usage:

Use always context manager, for example for GET:

``` python
>>> from patata.models import Request
>>> from collections import deque
>>> 
>>> def mygen():
...     for i in range(10_000):
...          yield Request(id_=i, url="http://localhost:12345/")
... 
>>> with Patata() as client:
...     responses = client.http("get", mygen())
...     _ = deque(responses)
... 
patata              INFO      Start processing requests with Patata parameters:
patata              INFO        method:             GET
patata              INFO        num_workers:        8
patata              INFO        multiprocessing:    True
patata              INFO        queue_max_size:     100000
patata              INFO        input_chunk_size:   10000
patata              INFO        pool_submit_size:   1000
patata              INFO      All requests processed:
patata              INFO        Total requests:     10000
patata              INFO        Total time (s):     5.00
patata              INFO        Requests/s:         1998.37
>>> _.pop()
Response(id_=9000, status_code=200, data={'message': 'Hello world!'})
```

In this example are providing a generator as input but you can provide any kind of iterable.

You can also provide callbacks to process the responses in each process before being yielded, so you can add your own post-processing of the responses taking benefit of multiprocessing. Example:
``` python
>>> from patata import Patata
>>> from patata.models import Request
>>> from collections import deque
>>> 
>>> def mycallback(response):
...     response.id_ = 1337
...     return response
... 
>>> def mygen():
...     for i in range(10_000):
...          yield Request(id_=i, url="http://localhost:12345/")
... 
>>> with Patata() as client:
...     responses = client.http("get", mygen(), callbacks=[mycallback])
...     _ = deque(responses)
... 
patata              INFO      Start processing requests with Patata parameters:
patata              INFO        method:             GET
patata              INFO        num_workers:        8
patata              INFO        multiprocessing:    True
patata              INFO        queue_max_size:     100000
patata              INFO        input_chunk_size:   10000
patata              INFO        pool_submit_size:   1000
patata              INFO      All requests processed:
patata              INFO        Total requests:     10000
patata              INFO        Total time (s):     4.89
patata              INFO        Requests/s:         2046.95
>>> _.pop()
Response(id_=1337, status_code=200, data={'message': 'Hello world!'})
```

For doing a POST:

Let's imagine we have this FastAPI endpoint:
``` python
@app.post("/")
async def root(data: dict):
    return data
```

We can consume the POST endpoint like this:
``` python
>>> from patata import Patata
>>> from patata.models import Request
>>> 
>>> requests = [
...     Request(id_=1, url="http://localhost:12345/", data={"hello": "POST 1"}),
...     Request(id_=2, url="http://localhost:12345/", data={"hello": "POST 2"}),
... ]
>>> with Patata(verbose=False) as client:
...     responses = client.http("post", requests)
...     for response in responses:
...         print(response)
... 
id_=2 status_code=200 data={'hello': 'POST 2'}
id_=1 status_code=200 data={'hello': 'POST 1'}
```

## Parameters

You can configure some parameters:

[patata.Patata](https://github.com/oalfonso-o/patata/blob/main/patata/client.py#L24) parameters:

- `num_workers`:
    - type: int
    - required: False
    - default: os.cpu_count()
    - description: Number of processes to open with multiprocessing
- `queue_max_size`:
    - type: int
    - required: False
    - default: 100.000
    - description: Maximum number of items that can be enqueued. This default number proved to not blow up the memory and to have enough items in the queue to have always work to do with 8 processes. Feel free to adjust it, just watch out the memory usage.
- `input_chunk_size`:
    - type: int
    - required: False
    - default: 10.000
    - description: This is the size of the chunks for the input. We will be reading the input iterator in chunks of this size up to `queue_max_size`.
- `pool_submit_size`:
    - type: int
    - required: False
    - default: 1.000
    - description: Each chunk of `input_chunk_size` will also be chunked to minor chunks of this size before being submited to the pool. The workers will be consuming chunks of this size and each of these chunks will be requested in an event loop.
- `verbose_level`:
    - type: int
    - required: False
    - default: 1
    - description: Configure the level of logging. Possible values:
        - 0: "mute" means not a single line of log will appear
        - 1: "info" means that only the start params + num of processed lines + end summary will be logged
        - 2: "debug" means that all exceptions will be logged, this includes error responses from the remote server



[patata.Patata.http](https://github.com/oalfonso-o/patata/blob/main/patata/client.py#L42)

Parameters:

- `method`:
    - type: str
    - required: True
    - description: Specify the method of the requests. Valid values: GET, POST.
- `requests`:
    - type: Iterable[Tuple[int, str]]
    - required: True
    - description: Provide the tuples containing the ID of the request and the URL to be requested.
- `callbacks`:
    - type: Iterable[Tuple[int, str]]
    - required: False
    - default: []
    - description: Callables that will be executed for each response, they must expect receiving a Response and must return another Response
- `retries`:
    - type: Optional[int]
    - required: False
    - default: 1
    - description: Total amount of requests to perform if the response is an error. Default is 1 which means doing the request only once, so no retries.

Response: Generator[Tuple[int, str], None, None]. For each input tuple an output tuple will be returned containing the same ID + the JSON of the response.


TODO:
- tests
- add flag to specify how many requests can fail, this will need to specify also which codes are "ok" or which are "not ok" do decide when to increment this count and decide to stop
- include the missing methods like PUT, DELETE, etc