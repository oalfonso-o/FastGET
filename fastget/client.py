import asyncio
from collections.abc import Iterable
import concurrent.futures
import itertools
import logging
import os
import time
from typing import List, Tuple, Generator, Optional

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format=("%(asctime)-25s" "%(name)-20s" "%(levelname)-10s" "%(message)s"),
)
logger = logging.getLogger("fastget")


class FastGET:

    NUM_CPUS: Optional[int] = os.cpu_count() or 1
    QUEUE_MAX_SIZE: int = 100_000
    INPUT_CHUNK_SIZE: int = 10_000
    POOL_SUBMIT_SIZE: int = 1_000

    def __init__(
        self,
        num_workers: int = 0,
        pool_submit_size: int = 0,
        input_chunk_size: int = 0,
        queue_max_size: int = 0,
    ):
        self.responses: List[Tuple[int, str]] = []
        self.total_processed_requests: int = 0
        self.num_workers = num_workers or self.NUM_CPUS
        self.queue_max_size = queue_max_size or self.QUEUE_MAX_SIZE
        self.input_chunk_size = input_chunk_size or self.INPUT_CHUNK_SIZE
        self.pool_submit_size = pool_submit_size or self.POOL_SUBMIT_SIZE
        self.executor = None

    def get(
        self, ids_and_urls: Iterable[Tuple[int, str]]
    ) -> Generator[Tuple[int, str], None, None]:
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

        if self.responses or self.total_processed_requests:
            raise Exception(
                "This client is in use, the same client can't be used concurrently"
            )

        logger.info("Start processing requests with FastGET parameters:")
        logger.info(f"  num_workers:        {self.num_workers}")
        logger.info(f"  queue_max_size:     {self.queue_max_size}")
        logger.info(f"  input_chunk_size:   {self.input_chunk_size}")
        logger.info(f"  pool_submit_size:   {self.pool_submit_size}")

        init_time = time.time()
        urls_in_queue = 0
        urls_chunks = self._chunker(ids_and_urls, self.input_chunk_size)

        for urls_chunk in urls_chunks:

            if urls_in_queue < self.queue_max_size:
                chunks = self._chunker(urls_chunk, self.pool_submit_size)
                for chunk in chunks:
                    urls = list(chunk)
                    future = self.executor.submit(Requester.run, urls)
                    future.add_done_callback(self._future_done_callback)
                    urls_in_queue += len(urls)

            for _ in range(len(self.responses)):
                urls_in_queue -= 1
                self.total_processed_requests += 1
                yield self.responses.pop()

            if (
                self.total_processed_requests % 10_000 == 0
                and self.total_processed_requests
            ):
                logger.info(
                    f"Total processed requests: {self.total_processed_requests}..."
                )

        while urls_in_queue:
            if self.responses:
                urls_in_queue -= 1
                self.total_processed_requests += 1
                yield self.responses.pop()

        if self.responses:
            raise Exception("We should have returned everything!")

        total_time = time.time() - init_time
        logger.info("All requests processed:")
        logger.info(f"  Total requests:     {self.total_processed_requests}")
        logger.info(f"  Total time (s):     {total_time:.2f}")
        logger.info(
            f"  Requests/s:         {(self.total_processed_requests/total_time):.2f}"
        )
        self.total_processed_requests = 0

    def __enter__(self):
        self.executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=self.num_workers
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.executor.shutdown(wait=True)
        return False

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
    async def _make_requests_async(
        cls, urls: List[Tuple[int, str]]
    ) -> List[Tuple[int, str]]:
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
                logger.exception(e)
            return (id_, response_json)
