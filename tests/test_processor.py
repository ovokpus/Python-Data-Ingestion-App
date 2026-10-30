from ingest.models import Post
from ingest.processor import DataProcessor


def test_extracts_named_entities_and_skips_numbers_and_dates():
    p = DataProcessor()
    out = p.process_message(Post(content='Apple opened an office in Toronto on 5 May 2021 with 300 staff.', publication='Tech', id='p1'))
    ents = set(out.entities)
    assert {'apple', 'toronto'} <= ents
    assert not any(e in ents for e in ('5 may 2021', '300'))  # DATE/CARDINAL skipped (label_ fix)
    assert out.post_ids == ['p1'] and out.publication == 'Tech'
