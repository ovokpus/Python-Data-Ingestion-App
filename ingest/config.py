'''Runtime settings, all from environment variables (see .env.example).'''
import os

QUEUE_HOST = os.environ.get('QUEUE_HOST', '127.0.0.1')
QUEUE_BIND = os.environ.get('QUEUE_BIND', '127.0.0.1')
QUEUE_PORT = int(os.environ.get('QUEUE_PORT', '50000'))
# Shared secret for the multiprocessing queue manager (not a user credential). Override outside local demos.
QUEUE_AUTHKEY = os.environ.get('QUEUE_AUTHKEY', 'ingestbackend').encode()

# postgres (default) | firestore (original course target, needs google-cloud-firestore + credentials) | none
DATA_STORAGE = os.environ.get('DATA_STORAGE', 'postgres')
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://ingest:ingest@localhost:5434/ingest')

# Optional: if set, POST /post/enqueue requires header `access_token: <value>`. Unset = no auth (local demo).
API_KEY = os.environ.get('INGEST_API_KEY') or None

# Workers flush their aggregate cache after this many posts, or after this many idle seconds.
FLUSH_IDLE_SECONDS = float(os.environ.get('FLUSH_IDLE_SECONDS', '2'))
