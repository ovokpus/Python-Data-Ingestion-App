#######################################################################################################
''' 
This module provides us with a drainable multiprocess aware message queue
'''
#######################################################################################################


from multiprocessing import Event, Queue
from multiprocessing.managers import BaseManager
from queue import Empty
from typing import Any, List

from ingest.debugging import app_logger as log


class QueueWrapper(object):
    def __init__(self, name: str, q: Queue = None, prevent_writes: Event = None):
        self.name = name
        self.q: Queue = q or Queue()
        self._prevent_writes: Event = prevent_writes or Event()

    def get(self) -> Any:
        ''' 
        This call blocks until it gets a message from the queue.
        If the queue is drained, it returns the sentinal string STOP.
        If the queue is closed while this call is blocking, it'ss return STOP
        '''
        if self.is_drained:
            return 'STOP'
        try:
            return self.q.get()
        except Exception as ex:
            log.info(f'q.get() interrupted: {ex}')
            return 'STOP'

    def put(self, obj: object):
        if self.is_writable:
            log.debug('putting message in queue')
            self.q.put(obj)

    def put_many(self, objs: List[object]):
        for obj in objs:
            self.put(obj)

    def prevent_writes(self):
        ''' 
        Prevent external writes to the queue.
        This is useful for shutting down, or dealing with back pressure.
        '''
        log.debug(f'preventing writes to the {self.name} queue')
        self._prevent_writes.set()

    @property
    def is_writable(self) -> bool:
        '''
        Read-only property indicating if the queue is writable.
        '''
        return not self._prevent_writes.is_set()

    @property
    def is_drained(self) -> bool:
        ''' if the queue is not writable and is empty the queue is draining'''
        return not self.is_writable and self.q.empty()

    @property
    def empty(self) -> bool:
        ''' 
        Read-only property indicating if the queue is empty.
        '''
        return self.q.empty()


class QueueManager(BaseManager):
    pass


def register_manager(name: str, queue: QueueWrapper = None):
    def callable():
        return queue
    if queue:
        QueueManager.register(name, callable=callable())
    else:
        QueueManager.register(name)


def create_queue_manager(port: int) -> QueueManager:
    '''Binds to 127.0.0.1 on the given port. 
    Using localhost on at least Debian systems results in extremly slow put() calls.
    '''
    return QueueManager(address=('127.0.0.1', port), authkey=b'ingestbackend')
