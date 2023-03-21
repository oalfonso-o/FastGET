import os
import concurrent.futures
import asyncio
import itertools
from typing import List, Tuple, Generator

import aiohttp


class FastGET:

    NUM_CPUS: int = os.cpu_count()
    SINGLE_SUBMIT_SIZE: int = 5_000
    POOL_SUBMIT_SIZE: int = 50_000
    QUEUE_MAX_SIZE: int = 100_000

    def __init__(
        self,
        num_workers: int = None,
        single_submit_size: int = None,
        pool_submit_size: int = None,
        queue_max_size: int = None,
    ) -> Generator[List[Tuple[int, str]], None, None]:
        self.responses = []
        self.num_workers = num_workers or self.NUM_CPUS
        self.single_submit_size = single_submit_size or self.SINGLE_SUBMIT_SIZE
        self.pool_submit_size = pool_submit_size or self.POOL_SUBMIT_SIZE
        self.queue_max_size = queue_max_size or self.QUEUE_MAX_SIZE

    def get(self, ids_and_urls: List[Tuple[int, str]]):
        """Uses multiprocessing and aiohttp to retrieve GET requests in parallel and concurrently

        Expects a list of tuples, each tuple containing the ID of the request and the URL.

        Example:
        [(0, "https://www.google.com"), (1, "https://www.youtube.com")]

        It only supports GET requests without parameters, only the URL so the query params should
        be already URL encoded.

        Yields tuples with the same format: ID + the JSON response, as soon as a response is
        received.

        Example:
        >>> client = FastGET()
        >>> responses = client.get([(0, "http://0.0.0.0:12345")])
        >>> next(responses)
        (0, {'message': 'Hello Single View API user!'})

        Is not thread safe, a single client must not be used in parallel or in another thread as
        the responses are stored in the instance variable `responses` and are yielded from there.
        Using the same client to perform two `get` operations in parallel will lead to mixing the
        responses.

        There's no need for it but can be used with context manager to avoid reusing it, but the
        manager is dummy, it just allows you to open it with the with statement but the proper
        context is managed by concurrent.futures.ProcessPoolExecutor and aiohttp.ClientSession when
        used.

        Example:
        >>> with FastGET() as client:
        ...     responses = client.get([(0, "http://0.0.0.0:12345")])
        ...     next(responses)
        ...
        (0, {'message': 'Hello Single View API user!'})
        """
        urls_in_queue = 0
        urls_chunks = self._chunker(ids_and_urls, self.pool_submit_size)

        with concurrent.futures.ProcessPoolExecutor(max_workers=self.num_workers) as executor:

            for urls_chunk in urls_chunks:

                if urls_in_queue < self.queue_max_size:
                    chunks = self._chunker(urls_chunk, self.pool_submit_size)
                    for chunk in chunks:
                        urls = list(chunk)
                        future = executor.submit(Requester.run, urls)
                        future.add_done_callback(self._future_done_callback)
                        urls_in_queue += len(urls)

                for _ in range(len(self.responses)):
                    urls_in_queue -= 1
                    yield self.responses.pop()

            while urls_in_queue:
                if self.responses:
                    urls_in_queue -= 1
                    yield self.responses.pop()

            if self.responses:
                raise Exception("We should have returned everything!")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @staticmethod
    def _chunker(iterable, size: int):
        iterator = iter(iterable)
        for first in iterator:
            yield itertools.chain([first], itertools.islice(iterator, size - 1))

    def _future_done_callback(self, future):
        results = future.result()
        self.responses.extend(results)


class Requester:

    @classmethod
    def run(cls, urls: List[Tuple[int, str]]) -> List[Tuple[int, str]]:
        responses = asyncio.run(cls._make_requests_async(urls))
        return responses

    @classmethod
    async def _make_requests_async(cls, urls: List[Tuple[int, str]]) -> List[Tuple[int, str]]:
        async with aiohttp.ClientSession() as session:
            tasks = []
            for id_, url in urls:
                task = asyncio.ensure_future(cls._make_request_async(session, id_, url))
                tasks.append(task)
            results = await asyncio.gather(*tasks)
        return results

    @staticmethod
    async def _make_request_async(session, id_: int, url: str) -> Tuple[int, str]:
        async with session.get(url) as response:
            try:
                response_json = await response.json()
            except Exception as e:
                print(e)
            return (id_, response_json)
