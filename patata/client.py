import asyncio
import concurrent.futures
import itertools
import logging
import os
import time
from typing import List, Generator, Optional, Iterable, Callable

# from collections.abc import Iterable  # only for >=3.9

import aiohttp
from aiohttp_retry import RetryClient, ExponentialRetry

from .models import Request, Response
from .exceptions import ClientAlreadyInUseError, InternalPatataError, InvalidMethodError

logging.basicConfig(
    level=logging.INFO,
    format=("%(asctime)-25s" "%(name)-20s" "%(levelname)-10s" "%(message)s"),
)
logger = logging.getLogger("patata")

POST = "POST"
GET = "GET"
VALID_METHODS = [
    GET,
    POST,
]
RETRY_STATUSES = {status for status in range(300, 600) if status not in [418, 429]}


class Patata:
    NUM_CPUS: Optional[int] = os.cpu_count() or 1
    QUEUE_MAX_SIZE: int = 100_000
    INPUT_CHUNK_SIZE: int = 20_000
    POOL_SUBMIT_SIZE: int = 7_500

    def __init__(
        self,
        workers: int = 0,
        queue_max_size: int = 0,
        input_chunk_size: int = 0,
        pool_submit_size: int = 0,
        verbose: bool = True,
    ):
        self.responses: List[Response] = []
        self.total_processed_requests: int = 0
        self.workers = workers or self.NUM_CPUS
        self.queue_max_size = queue_max_size or self.QUEUE_MAX_SIZE
        self.input_chunk_size = input_chunk_size or self.INPUT_CHUNK_SIZE
        self.pool_submit_size = pool_submit_size or self.POOL_SUBMIT_SIZE
        self.verbose = verbose
        self.executor: Optional[concurrent.futures.ProcessPoolExecutor] = None
        if self.workers != 1:
            self.executor = concurrent.futures.ProcessPoolExecutor(
                max_workers=self.workers
            )

    def http(
        self,
        method: str,
        requests: Iterable[Request],
        callbacks: Iterable[Callable] = [],
        retries: int = 1,
    ) -> Generator[Response, None, None]:
        """Uses multiprocessing and aiohttp to retrieve GET or POST requests in parallel and
        concurrently

        Parameters
        ------------
            method: str
                GET or POST
            requests: Iterable[patata.Request]
                Iterable of Request objects containing the id, url and data
            callbacks: Optional[Iterable[Callable]] = None
                Callables that will be executed for each response, they must expect receiving a
                Response and must return another Response
            retries: Optional[int]
                Total amount of requests to perform if the response is an error. Default is 1 which
                means doing the request only once, so no retries.
        Return
        -----------
            responses : Generator[patata.Response, None, None]
                As soon as the response is ready it will be yielded. The response contains the id,
                the status_code and the json returned.

        Example of input requests:
        [
            Request(id_=0, url="https://www.google.com", data={}),
            Request(id_=1, url="http://localhost:12345", data={"key": "value"}),
        ]

        It only supports GET and POST methods.

        URL input parameters should come already url encoded.

        Example:
        >>> from patata import Patata, Request
        >>> with Patata() as client:
        ...     responses = client.http("get", [Request(id_=0, url="http://localhost:12345/", data={})])
        ...     next(responses)
        ...
        Response(id_=0, status_code=200, data={'message': 'Hello world'})

        If you use it without context manager you have to call the .close() to close the pool of
        processes.

        It is not thread safe, a single client must only be used in the main thread as the
        responses are stored in the instance variable `responses` and are yielded from there.
        Using the same client to perform two `patata.Patata.http` calls in parallel will lead to
        mixing the responses.
        """  # noqa E501
        if self.responses or self.total_processed_requests:
            raise ClientAlreadyInUseError(
                "This client is in use, the same client can't be used concurrently"
            )

        if self.verbose:
            logger.info("Start processing requests with Patata parameters:")
            logger.info(f"  method:             {method.upper()}")
            logger.info(f"  workers:            {self.workers}")
            logger.info(f"  multiprocessing:    {self.workers != 1}")
            logger.info(f"  queue_max_size:     {self.queue_max_size}")
            logger.info(f"  input_chunk_size:   {self.input_chunk_size}")
            logger.info(f"  pool_submit_size:   {self.pool_submit_size}")
            logger.info(f"  verbose:            {self.verbose}")

        init_time = time.time()
        requests_in_queue = 0
        requests_chunks = self._chunker(requests, self.input_chunk_size)

        for requests_chunk in requests_chunks:
            if requests_in_queue < self.queue_max_size:
                chunks = self._chunker(requests_chunk, self.pool_submit_size)
                for chunk in chunks:
                    requests = self._validate_input(chunk)
                    if self.executor:
                        future = self.executor.submit(
                            Requester.run,
                            method=method,
                            requests=requests,
                            callbacks=callbacks,
                            verbose=self.verbose,
                            retries=retries,
                        )
                        future.add_done_callback(self._future_done_callback)
                    else:  # run in the main thread
                        self.responses.extend(
                            Requester.run(
                                method=method,
                                requests=requests,
                                callbacks=callbacks,
                                verbose=self.verbose,
                                retries=retries,
                            )
                        )

                    requests_in_queue += len(requests)

            for _ in range(len(self.responses)):
                requests_in_queue -= 1
                self.total_processed_requests += 1
                yield self.responses.pop()

            self._log_process()

        while requests_in_queue:
            if self.responses:
                requests_in_queue -= 1
                self.total_processed_requests += 1
                self._log_process()
                yield self.responses.pop()

        if self.responses:
            raise InternalPatataError(
                "We should have returned everything!"
            )  # shouldn't happen

        if self.verbose:
            total_time = time.time() - init_time
            logger.info("All requests processed:")
            logger.info(f"  Total requests:     {self.total_processed_requests}")
            logger.info(f"  Total time (s):     {total_time:.2f}")
            logger.info(
                f"  Requests/s:         {(self.total_processed_requests/total_time):.2f}"
            )

        self.total_processed_requests = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def close(self):
        if self.executor:
            self.executor.shutdown(wait=True)

    @staticmethod
    def _chunker(iterable, size: int):
        iterator = iter(iterable)
        for first in iterator:
            yield itertools.chain([first], itertools.islice(iterator, size - 1))

    @staticmethod
    def _validate_input(chunk) -> List[Request]:
        requests = []
        for request in chunk:
            if isinstance(request, Request):
                requests.append(request)
            else:
                raise ValueError(f"Input {request} must be of type Request")
        return requests

    def _future_done_callback(self, future):
        results = future.result()
        self.responses.extend(results)

    def _log_process(self):
        if (
            self.verbose
            and self.total_processed_requests % self.input_chunk_size == 0
            and self.total_processed_requests
        ):
            logger.info(f"Total processed requests: {self.total_processed_requests}...")


class Requester:
    @classmethod
    def run(
        cls,
        method: str,
        requests: List[Request],
        callbacks: Iterable[Callable],
        verbose: bool = True,
        retries: int = 1,
    ) -> List[Response]:
        if method.upper() not in VALID_METHODS:
            raise InvalidMethodError(
                f"The method {method} is not valid. Valid methods: {VALID_METHODS}"
            )

        responses = asyncio.run(
            cls._make_requests_async(method.lower(), requests, verbose, retries)
        )

        for response in responses:
            for callback in callbacks:
                response = callback(response)

        return responses

    @classmethod
    async def _make_requests_async(
        cls,
        method: str,
        requests: List[Request],
        verbose: bool = True,
        retries: int = 1,
    ) -> List[Response]:
        async with aiohttp.ClientSession() as session:
            retry_options = ExponentialRetry(attempts=retries)
            retry_client = RetryClient(session, retry_options=retry_options)
            tasks = []
            for request in requests:
                task = asyncio.ensure_future(
                    cls._make_request_async(retry_client, method, request, verbose)
                )
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
        return responses

    @staticmethod
    async def _make_request_async(
        retry_client: RetryClient,
        method: str,
        request: Request,
        verbose: bool = True,
    ) -> Response:
        client_method = getattr(retry_client, method)
        headers = {"accept": "application/json"}

        if method.upper() == POST and request.data:
            headers["Content-Type"] = "application/json"

        async with client_method(
            request.url,
            json=request.data,
            headers=headers,
        ) as response:
            response_json = {}
            try:
                status_code = response.status
                response_json = await response.json()
            except Exception as e:
                if verbose:
                    logger.exception(e)
            return Response(
                id_=request.id_, status_code=status_code, data=response_json
            )
