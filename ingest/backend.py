###############################################################################
'''
    This module handles the creation of worker and saver processes.
    Worker processes get items from a message queue and process them with a
    DataProcessor(). The processed data is put on a different message queue 
    which is consumed by the saver.
    
    Saver processes get items from a message queue and saves it (PostgreSQL by default, Firestore optional).
    
    INPUT QUEUE           Workers      OUTPUT QUEUE
    [................] -> Worker() --> [................] -> Saver() --> PostgreSQL
                      |_> Worker() _|                    |_> Saver() _|
                      |_> Worker() _|                    |_> Saver() _|
                      |_> Worker() _|
                      
'''
###############################################################################

import os
import signal
from collections import defaultdict
from multiprocessing import Process
from typing import Dict, List, Tuple

from .debugging import app_logger as log
from .messageq import QueueWrapper, create_queue_manager, register_manager
from .models import ProcessedPost
from . import config
from .persistence import persistable as get_persistable
from .processor import DataProcessor
from .shutdownwatcher import ShutdownWatcher


class Worker(Process):
    '''
    Worker is a multiproecessing.Process that is responsible for fetching data 
    from the input queue and extracting known entities.
    '''

    def __init__(self, inq: QueueWrapper, outq: QueueWrapper, cache_size: int = 25_000,
                 flush_idle_seconds: float = None, processor_factory=None):
        self.iq: QueueWrapper = inq
        self.oq: QueueWrapper = outq
        self._cache_size: int = cache_size
        self._flush_idle = config.FLUSH_IDLE_SECONDS if flush_idle_seconds is None else flush_idle_seconds
        self._processor_factory = processor_factory or DataProcessor
        self._count: int = 0
        self._pending: int = 0
        self.reset_cache()
        super(Worker, self).__init__()

    def shutdown(self, *args):
        log.info('Shutting down worker')
        # Original put STOP on a non-existent `outq` attribute; the worker's own input queue must get it.
        self.iq.q.put('STOP')

    def count(self, incr_num: int = None) -> int:
        '''Count increments the counter by the given value and returns the total.
        If no value is given, the current count is returned.
        '''
        if incr_num:
            self._count += incr_num
        return self._count

    def reset_cache(self):
        self._cache = defaultdict(ProcessedPost)
        self._pending = 0

    def cache(self, msg: ProcessedPost) -> int:
        '''Caches messages until flush_cache is called.
        Returns the number of messages currently cached (the original returned the lifetime total, so
        `== cache_size` was only ever true once and later posts sat in memory until shutdown).
        '''
        self._cache[msg.pub_key] += msg
        self.count(1)
        self._pending += 1
        return self._pending

    def flush_cache(self):
        '''Flushes the cache to the output queue.'''
        if not self._cache:
            return
        log.info(f'Flushing cache with {len(self._cache)} publications / {self._pending} posts')
        for post in self._cache.values():
            self.oq.put_many(post.transform_for_database(worker_pid=os.getpid()))
        self.reset_cache()

    def consume(self, processor):
        '''Processes messages until STOP, flushing when the cache is full or the queue has been idle.'''
        while True:
            msg = self.iq.get(timeout=self._flush_idle)
            if msg == 'STOP':
                break
            if msg is None:  # idle: publish what we have so results show up promptly
                self.flush_cache()
                continue
            if self.cache(processor.process_message(msg)) >= self._cache_size:
                self.flush_cache()
        self.flush_cache()

    def run(self):
        # Register the shutdown handler for this process
        signal.signal(signal.SIGTERM, self.shutdown)
        '''Only the worker processes need to use the data processor. The data processor uses Spacy for its processing.
        Spacy can take up a bit of memory when loaded. The amount depends on which model is used. If we instantiate
        in __init__ the process that creates Workers ends up using more memory than needed.'''
        self.consume(self._processor_factory())
        # Leaving the process with a status code of 0, if all went well
        exit(0)


class Saver(Process):
    '''Saver pulls messages off the queue and passes the message and client to the persist method.'''

    def __init__(self, q: QueueWrapper, client_factory, persist_fn):
        assert callable(persist_fn)
        self.q: QueueWrapper = q
        self.client_factory = client_factory
        self.persist_fn = persist_fn
        super(Saver, self).__init__()

    def shutdown(self, *args):
        log.info('Shutting down saver')
        self.q.q.put('STOP')

    def run(self):
        signal.signal(signal.SIGTERM, self.shutdown)
        client = self.client_factory()  # one connection per process, created after fork
        for msg in iter(self.q.get, 'STOP'):
            log.debug(f'Persisting message: {msg}')
            try:
                self.persist_fn(client, *msg)
            except Exception as ex:  # keep the saver alive; one bad message must not stop persistence
                log.error(f'Failed to persist {msg[:2]}: {ex}')
        exit(0)


def start_processes(proc_num: int, proc: Process, proc_args: List[object]) -> List[Process]:
    '''Instantiates and starts the given process'''
    log.info(f'Starting {proc_num} worker processes')
    procs = [proc(*proc_args) for _ in range(proc_num)]
    for p in procs:
        # breakpoint()
        p.start()
    return procs


def shutdown(q: QueueWrapper, procs: List[Process]):
    '''Shuts downrs the given processes using the following steps:
    1. Disable writes to the given QueueWrappper
    2. Send SIGTERM signals to each of the given processes
    3. Calls join on the procs, blocking until they are all done
    '''
    q.prevent_writes()
    log.info(f"sending SIGTERM to processes")
    [os.kill(p.pid, signal.SIGTERM) for p in procs]
    log.info(f"joining processes")
    [p.join() for p in procs]


def register_shutdown_handlers(queues, processes):
    '''Create shutdown handlers to be kicked off on exit'''
    def shutdown_gracefully():
        for args in zip(queues, processes):
            shutdown(*args)

    import atexit
    atexit.register(shutdown_gracefully)


def main():
    pcount = max(1, (os.cpu_count() or 2) - 7)  # the original could go to 0 or negative
    parser_arguments = [
        ('--iproc_num', {'type': int, 'default': pcount,
         'help': 'Number of input queue workers'}),
        ('--oproc_num', {'type': int, 'default': pcount,
         'help': 'Number of output queue workers'}),
        ('--iport', {'type': int, 'default': config.QUEUE_PORT,
         'help': 'Input queue port cross-process messaging'}),
        ('--no_persistence', {'action': 'store_true',
         'help': 'Do not persist data (same as DATA_STORAGE=none)'}),
        ('--agg_cache_size', {'type': int, 'default': 25_000,
         'help': 'Number of messages to cache before persisting'})
    ]

    import argparse
    parser = argparse.ArgumentParser()
    for name, args in parser_arguments:
        parser.add_argument(name, **args)

    args = parser.parse_args()

    iproc_num = args.iproc_num
    oproc_num = args.oproc_num
    iport = args.iport
    cache_sz = args.agg_cache_size
    # A tuple containing the db client and method for persisting message
    # For testing, the no_persistence flas allows us to use a null client with a no op function
    persistable = get_persistable('none' if args.no_persistence else None)
    if not args.no_persistence and config.DATA_STORAGE == 'postgres':
        persistable[0]().close()  # fail fast and create the schema before workers start

    # Set up the input queue, aggregate queue and output queue
    iq = QueueWrapper(name="iqueue")
    oq = QueueWrapper(name="oqueue")

    # Register and start the input queue manager for remote connections
    # This allows the frontend to put messages on the queue
    register_manager("iqueue", iq)
    iserver = create_queue_manager(iport)
    log.info(f'Input queue listening on {config.QUEUE_BIND}:{iport}')
    iserver.start()

    # Start the worker/ saver processes
    iprocs = start_processes(iproc_num, Worker, [iq, oq, cache_sz])
    oprocs = start_processes(oproc_num, Saver, [oq, *persistable])

    # Set up the shutdown handlers to gracefully shutdown the processes
    register_shutdown_handlers([iq, oq], [iprocs, oprocs])
    
    # from .models import Post
    # iq.put(Post(content='John has $1000 for a new Android product', publication='me'))
    # iq.put(Post(content='Ben has for an Android product', publication='me'))

    with ShutdownWatcher() as watcher:
        watcher.serve_forever()
    exit(0)


if __name__ == '__main__':
    main()
