################################################################################################################
"""
This module provides methods for persisting processed data to long-term storage.

DATA_STORAGE=postgres (default, used by docker compose), firestore (the original course target) or none.
"""
################################################################################################################

from typing import Callable, Tuple

from . import config
from .debugging import app_logger as log
from .models import FLUSH

SCHEMA = """
CREATE TABLE IF NOT EXISTS posts (
  id TEXT PRIMARY KEY,
  publication TEXT NOT NULL,
  title TEXT,
  chars INTEGER NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at TIMESTAMPTZ,
  flush_id INTEGER
);
CREATE TABLE IF NOT EXISTS publications (
  pub_key TEXT PRIMARY KEY,
  name TEXT,
  article_count INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS entities (
  pub_key TEXT NOT NULL,
  word TEXT NOT NULL,
  count INTEGER NOT NULL,
  PRIMARY KEY (pub_key, word)
);
CREATE TABLE IF NOT EXISTS flushes (
  id SERIAL PRIMARY KEY,
  pub_key TEXT NOT NULL,
  worker_pid INTEGER,
  posts INTEGER NOT NULL,
  entities INTEGER NOT NULL,
  flushed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def persist_no_op(*args, **kwargs):
    pass


# ---------------------------------------------------------------- PostgreSQL
def pg_connect(url: str = None):
    import psycopg
    conn = psycopg.connect(url or config.DATABASE_URL, autocommit=True)
    conn.execute(SCHEMA)
    return conn


def persist_postgres(conn, pubname, collname, doc_id, document_dict):
    if collname == FLUSH:
        d = document_dict
        row = conn.execute("INSERT INTO flushes (pub_key, worker_pid, posts, entities) VALUES (%s,%s,%s,%s) RETURNING id",
                           (pubname, d.get('worker_pid'), d['posts'], d['entities'])).fetchone()
        if d['post_ids']:
            conn.execute("UPDATE posts SET processed_at = now(), flush_id = %s WHERE id = ANY(%s)", (row[0], d['post_ids']))
        conn.execute("INSERT INTO publications (pub_key, name) VALUES (%s, %s) ON CONFLICT (pub_key) DO UPDATE SET name = EXCLUDED.name",
                     (pubname, d.get('publication')))
    elif collname is None or doc_id is None:
        conn.execute("INSERT INTO publications (pub_key, article_count) VALUES (%s, %s) ON CONFLICT (pub_key) "
                     "DO UPDATE SET article_count = publications.article_count + EXCLUDED.article_count",
                     (pubname, document_dict['count']))
    else:
        conn.execute("INSERT INTO entities (pub_key, word, count) VALUES (%s, %s, %s) ON CONFLICT (pub_key, word) "
                     "DO UPDATE SET count = entities.count + EXCLUDED.count",
                     (pubname, document_dict['word'], document_dict['count']))
    log.debug('persisted %s/%s', pubname, collname)


# ---------------------------------------------------------------- Firestore (original target)
def get_database_client():
    from google.cloud import firestore  # optional dependency: pip install '.[firestore]'
    return firestore.Client()


def persist(client, pubname, collname, doc_id, document_dict):
    from google.cloud import firestore
    if collname == FLUSH:
        return  # flush records are only tracked in the PostgreSQL backend
    # Check these values to determine if this is a Publication count incrementor message
    if collname is None or doc_id is None:
        increment_publication(client, pubname, document_dict['count'])
    else:
        document_dict = {**document_dict, 'count': firestore.Increment(document_dict['count'])}
        pubdoc = client.collection(u'publications').document(pubname)
        # Firestore document ids cannot contain '/', so use a stable digest of the word
        import hashlib
        wrddoc = pubdoc.collection(collname).document(hashlib.sha1(doc_id.encode()).hexdigest())
        wrddoc.set(document_dict, merge=True)
        log.debug('incremented word count for %s', doc_id)


def increment_publication(client, pubname, count):
    from google.cloud import firestore
    pubdoc = client.collection(u'publications').document(pubname)
    pubdoc.set({'count': firestore.Increment(count)}, merge=True)
    log.debug('incremented publication count for %s', pubname)


def persistable(storage: str = None) -> Tuple[Callable, Callable]:
    '''(client_factory, persist_fn). The client is created inside each Saver process, since database
    connections must not be shared across fork().'''
    storage = storage or config.DATA_STORAGE
    if storage == 'postgres':
        return pg_connect, persist_postgres
    if storage == 'firestore':
        return get_database_client, persist
    if storage == 'none':
        return (lambda: None), persist_no_op
    raise ValueError(f'unknown DATA_STORAGE {storage!r}')

