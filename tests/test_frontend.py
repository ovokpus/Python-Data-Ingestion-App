from fastapi.testclient import TestClient

from ingest import config, frontend


class FakeQueue:
    def __init__(self):
        self.items = []

    def put(self, obj):
        self.items.append(obj)


def client(monkeypatch, queue=None):
    q = queue or FakeQueue()
    frontend.app.dependency_overrides[frontend.iqueue] = lambda: q
    monkeypatch.setattr(frontend, 'record_post', lambda post: None)
    return TestClient(frontend.app), q


def test_enqueue_returns_id_and_queues(monkeypatch):
    c, q = client(monkeypatch)
    monkeypatch.setattr(config, 'API_KEY', None)
    r = c.post('/post/enqueue', json={'publication': 'P', 'content': 'hello'})
    assert r.status_code == 201 and r.json()['status'] == 'queued'
    assert q.items[0].id == r.json()['id']


def test_api_key_enforced_only_when_configured(monkeypatch):
    c, _ = client(monkeypatch)
    monkeypatch.setattr(config, 'API_KEY', 'k')
    assert c.post('/post/enqueue', json={'publication': 'P', 'content': 'x'}).status_code == 403
    assert c.post('/post/enqueue', json={'publication': 'P', 'content': 'x'}, headers={'access_token': 'k'}).status_code == 201


def test_health_degraded_without_db_or_queue(monkeypatch):
    frontend.app.dependency_overrides.clear()
    monkeypatch.setattr(frontend, 'rows', lambda *a: (_ for _ in ()).throw(ConnectionError()))
    monkeypatch.setattr(config, 'QUEUE_HOST', '127.0.0.1')
    monkeypatch.setattr(config, 'QUEUE_PORT', 1)
    frontend.iqueue.reset()
    body = TestClient(frontend.app).get('/health').json()
    assert body['status'] == 'degraded' and not body['database'] and not body['queue']
