from collections import Counter

from ingest.models import FLUSH, Post, ProcessedPost


def test_aggregation_and_database_messages():
    agg = ProcessedPost()
    agg += ProcessedPost(publication='Tech Ledger', entities=Counter({'apple': 2, 'tim cook': 1}), post_ids=['a'])
    agg += ProcessedPost(publication='Tech Ledger', entities=Counter({'apple': 1}), post_ids=['b'])
    msgs = agg.transform_for_database(worker_pid=7)
    assert msgs[0] == ('tech ledger', 'ent', 'apple', {'word': 'apple', 'count': 3})
    assert ('tech ledger', None, None, {'count': 2}) in msgs
    flush = msgs[-1]
    assert flush[1] == FLUSH and flush[3]['post_ids'] == ['a', 'b'] and flush[3]['entities'] == 2 and flush[3]['worker_pid'] == 7


def test_entity_key_is_stable_across_processes():
    # the original used hash(word), which differs between interpreter processes
    msgs = ProcessedPost(publication='x', entities=Counter({'nasa': 1})).transform_for_database()
    assert msgs[0][2] == 'nasa'


def test_defaults_are_not_shared():
    a, b = ProcessedPost(), ProcessedPost()
    a.entities['x'] += 1
    assert b.entities == Counter()


def test_post_requires_content():
    import pydantic
    import pytest
    with pytest.raises(pydantic.ValidationError):
        Post(content='', publication='p')
