# FastGET: easy parallel and concurrent GET requests

The idea of this package is to wrap `multiprocessing` and `async` concurrency and allow the user to perform thousands of requests in parallel and concurrently without having to worry about pools of processes and async event loops.

It only supports GET requests. More methods could be implemented but this initial version is as simple as it. Also implementing other methods will break the beauty of the name.

The input is an iterable and the output is a generator. As soon as the requests get answer they are yielded until all the requests of the input are done.

Each element of the iterable is a tuple with `Id` and `URL`.
Each element yielded is a tuple with the same `Id` and the json response. With the `Id` you can
later map the responses.

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
pip install fastget
```

## Usage:

Use always context manager:

``` python
>>> from fastget import FastGET
>>> with FastGET() as client:
...     responses = client.get([(1, "http://localhost:12345"), (2, "http://localhost:12345")])
...     for response in responses:
...         print(response)
... 
fastget             INFO      Start processing requests with FastGET parameters:
fastget             INFO        num_workers:        8
fastget             INFO        single_submit_size: 5000
fastget             INFO        pool_submit_size:   50000
fastget             INFO        queue_max_size:     100000
(2, {'message': 'Hello World!'})
(1, {'message': 'Hello World!'})
fastget             INFO      All requests processed:
fastget             INFO        Total requests: 2
fastget             INFO        Total time:     0.05848503112792969
fastget             INFO        Requests/s:     34.16530770170651
```

You can provide a generator to don't blow up the memory:
``` python
>>> from fastget import FastGET
>>> from collections import deque
>>> 
>>> def mygen():
...     for i in range(100_000):
...          yield (i, "http://localhost:12345")
... 
>>> with FastGET() as client:
...     responses = client.get(mygen())
...     _ = deque(responses)
... 
fastget             INFO      Start processing requests with FastGET parameters:
fastget             INFO        num_workers:        8
fastget             INFO        queue_max_size:     100000
fastget             INFO        input_chunk_size:   10000
fastget             INFO        pool_submit_size:   1000
fastget             INFO      All requests processed:
fastget             INFO        Total requests:     100000
fastget             INFO        Total time (s):     36.35
fastget             INFO        Requests/s:         2750.93
```

## Parameters

You can configure some parameters like the amount of workers or how the client chunks the input:

[fastget.FastGET](https://github.com/oalfonso-o/FastGET/blob/main/fastget/client.py#L24) parameters:

- `num_workers`:
    - type: int
    - default: os.cpu_count()
    - description: Number of processes to open with multiprocessing
- `queue_max_size`:
    - type: int
    - default: 100.000
    - description: Maximum number of items that can be enqueued. This default number proved to not blow up the memory and to have enough items in the queue to have always work to do with 8 processes. Feel free to adjust it, just watch out the memory usage.
- `input_chunk_size`:
    - type: int
    - default: 10.000
    - description: This is the size of the chunks for the input. We will be reading the input iterator in chunks of this size up to `queue_max_size`.
- `pool_submit_size`:
    - type: int
    - default: 1.000
    - description: Each chunk of `input_chunk_size` will also be chunked to minor chunks of this size before being submited to the pool. The workers will be consuming chunks of this size and each of these chunks will be requested in an event loop.


[fastget.FastGET.get](https://github.com/oalfonso-o/FastGET/blob/main/fastget/client.py#L42)

Parameters:

- `ids_and_urls`:
    - type: Iterable[Tuple[int, str]]
    - required: True
    - description: Provide the tuples containing the ID of the request and the URL to be requested.

Response: Generator[Tuple[int, str], None, None]. For each input tuple an output tuple will be returned containing the same ID + the json of the response.