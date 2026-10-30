from queue import Queue

from ingest.backend import Worker
from ingest.messageq import QueueWrapper
from ingest.models import FLUSH, Post, ProcessedPost
from collections import Counter


class FakeProcessor:
    def process_message(self, post):
        return ProcessedPost(publication=post.publication, entities=Counter(post.content.split()), post_ids=[post.id])


def make(cache_size):
    iq, oq = QueueWrapper('i', q=Queue()), QueueWrapper('o', q=Queue())
    return Worker(iq, oq, cache_size=cache_size, flush_idle_seconds=0.05), iq, oq


def drain(q):
    out = []
    while not q.empty:
        out.append(q.get())
    return out


def test_flushes_every_cache_size_posts_not_just_once():
    w, iq, oq = make(cache_size=2)
    for i in range(5):
        iq.put(Post(content='a b', publication='P', id=str(i)))
    iq.put('STOP')
    w.consume(FakeProcessor())
    flushes = [m for m in drain(oq) if m[1] == FLUSH]
    # 2 + 2 on cache size, 1 on STOP (the original flushed once at 25k and never again)
    assert [f[3]['posts'] for f in flushes] == [2, 2, 1]
    assert w.count() == 5


def test_idle_timeout_flushes_partial_cache():
    w, iq, oq = make(cache_size=100)
    iq.put(Post(content='x', publication='P', id='1'))
    import threading
    threading.Timer(0.3, lambda: iq.put('STOP')).start()
    w.consume(FakeProcessor())
    flushes = [m for m in drain(oq) if m[1] == FLUSH]
    assert flushes and flushes[0][3]['post_ids'] == ['1']


def test_shutdown_puts_stop_on_input_queue():
    w, iq, oq = make(cache_size=1)
    w.shutdown()
    assert iq.get() == 'STOP'
