##########################################################################################################################
'''
This module provides a web based API for consuming Post data, plus read-only endpoints over the ingestion results.
Auth is optional: set INGEST_API_KEY to require an `access_token` header on POST /post/enqueue (unset = open, local demo).
'''
##########################################################################################################################

import uuid
from contextlib import contextmanager
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Security, status
from fastapi.security import APIKeyHeader

from . import config
from .debugging import app_logger as log
from .messageq import QueueWrapper, create_queue_manager, register_manager
from .models import Post

API_KEY_HEADER = APIKeyHeader(name='access_token', auto_error=False)
app = FastAPI(title='Data ingestion API')


class Connector:
    '''
    Connector is used to manage the connection to the input queue.
    The queue manager requires a call to .connect() to establish the connection.
    By reusing connections we can better manage our netwoking resources.
    '''

    def __init__(self):
        register_manager('iqueue')
        self.manager = None
        self.iqueue = None

    def reset(self):
        self.manager = None
        self.iqueue = None

    def __call__(self):
        '''
        returns a connected input queue proxy or raises 503 if the backend is unreachable.
        '''
        if self.iqueue:
            return self.iqueue
        try:
            self.manager = create_queue_manager(host=config.QUEUE_HOST)
            self.manager.connect()
            self.iqueue = self.manager.iqueue()
            return self.iqueue
        except Exception as ex:
            self.reset()
            log.error(f'input queue unavailable: {ex}')
            raise HTTPException(status_code=503, detail=f'Ingestion backend unavailable: {type(ex).__name__}')


iqueue = Connector()


def check_auth_header(api_key_header: str = Security(API_KEY_HEADER)):
    '''
    Check the API key header when INGEST_API_KEY is configured (the original hard-coded the value 'password').
    '''
    if config.API_KEY is None or api_key_header == config.API_KEY:
        return True
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid API key")


# ---------------------------------------------------------------- database helpers (read side + post log)
@contextmanager
def db():
    from .persistence import pg_connect
    conn = pg_connect()
    try:
        yield conn
    finally:
        conn.close()


def rows(sql: str, params: tuple = ()) -> list:
    with db() as conn:
        cur = conn.execute(sql, params)
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def record_post(post: Post):
    if config.DATA_STORAGE != 'postgres':
        return
    with db() as conn:
        conn.execute('INSERT INTO posts (id, publication, title, chars) VALUES (%s, %s, %s, %s)',
                     (post.id, post.publication, post.title, len(post.content)))


@app.post("/post/enqueue", status_code=status.HTTP_201_CREATED)
def create_post(post: Post, queue: QueueWrapper = Depends(iqueue), authenticated: bool = Depends(check_auth_header)):
    post.id = str(uuid.uuid4())
    try:
        record_post(post)
        queue.put(post)
    except Exception as ex:
        iqueue.reset()
        raise HTTPException(status_code=500, detail=f'Failed to enqueue post: {ex}')
    # The original returned an undefined name here (`return ex`), so every successful call was a 500.
    return {'id': post.id, 'status': 'queued'}


@app.get('/health')
def health():
    out = {'status': 'ok', 'database': False, 'queue': False}
    try:
        out.update(rows('SELECT (SELECT COUNT(*) FROM posts) AS posts, (SELECT COUNT(*) FROM posts WHERE processed_at IS NOT NULL) AS processed,'
                        ' (SELECT COUNT(*) FROM flushes) AS flushes')[0])
        out['database'] = True
    except Exception as ex:
        out['database_error'] = type(ex).__name__
    try:
        iqueue()
        out['queue'] = True
    except HTTPException:
        pass
    if not (out['database'] and out['queue']):
        out['status'] = 'degraded'
    return out


@app.get('/api/publications')
def publications():
    return rows('SELECT p.pub_key, p.name, p.article_count, (SELECT COUNT(*) FROM entities e WHERE e.pub_key = p.pub_key) AS distinct_entities'
                ' FROM publications p ORDER BY p.article_count DESC, p.pub_key')


@app.get('/api/entities')
def entities(publication: Optional[str] = None, limit: int = Query(15, ge=1, le=200)):
    if publication:
        return rows('SELECT word, count FROM entities WHERE pub_key = %s ORDER BY count DESC, word LIMIT %s',
                    (publication.strip().lower(), limit))
    return rows('SELECT word, SUM(count)::int AS count FROM entities GROUP BY word ORDER BY count DESC, word LIMIT %s', (limit,))


@app.get('/api/flushes')
def flushes(limit: int = Query(50, ge=1, le=500)):
    return rows('SELECT f.id, f.pub_key, p.name, f.worker_pid, f.posts, f.entities, f.flushed_at FROM flushes f'
                ' LEFT JOIN publications p USING (pub_key) ORDER BY f.id DESC LIMIT %s', (limit,))


@app.get('/api/posts')
def posts(limit: int = Query(100, ge=1, le=1000)):
    return rows('SELECT id, publication, title, chars, received_at, processed_at, flush_id,'
                ' EXTRACT(EPOCH FROM processed_at - received_at)::float AS latency_s FROM posts ORDER BY received_at DESC LIMIT %s', (limit,))
