# Business case: Python Data Ingestion App

**Which people, places and organisations each publication talks about, extracted as articles arrive.**

## Who buys this

The buyer is the data lead at a media-monitoring firm, a PR agency or a research team that tracks coverage across many publications. The users are analysts reading entity counts per publication.

## The problem and what it costs

Analysts want to know who and what each outlet is covering, without reading every article. Named-entity recognition does this automatically, but running NLP inside the request makes callers wait. This repo accepts posts immediately and does the NLP on separate worker processes. It uses spaCy's small English model `en_core_web_sm` ([spaCy models](https://spacy.io/models/en)). I found no reliable figure for analyst time spent on coverage tracking, so the model uses assumptions.

## How the repo solves it

| Need | What in the repo does it |
|---|---|
| Accept posts fast | `POST /post/enqueue` (FastAPI) logs the post and puts it on a multiprocessing queue, then returns at once |
| Extract entities | Worker processes run spaCy NER and count entities per publication, skipping dates, numbers and money |
| Batch the writes | Workers flush when their cache is full or the queue is idle, and savers upsert counts into Postgres and record each flush |
| Watch it | A React dashboard shows each run as it happens, and a simulator generates traffic |

## Use-case scenarios

All of these organisations are fictional.

1. **A PR agency** tracks how often each client is named by each trade publication every week.
2. **A policy think-tank** sees which organisations each outlet mentions most on a topic.
3. **A media-monitoring start-up** uses the queue-and-worker design as the core of its ingest service.

## ROI: a worked model (illustrative)

These figures are **illustrative assumptions** unless a source is linked.

| Input | Value | Basis |
|---|---|---|
| Articles a week | 5,000 | Assumption |
| Seconds an analyst would spend tagging entities per article | 30 | Assumption |
| Share of articles still read in full | 10% | Assumption |
| Analyst loaded cost an hour | US$50 | Assumption |

Without the pipeline, tagging takes about 42 hours a week. With it, analysts read 10%, about 4 hours. Saving: about **38 hours a week, about US$98,000 a year**. The assumption that matters most is whether anyone would tag entities by hand at all.

## Path to production

1. Replace the simulator with real feeds (RSS, a news API, a licensed archive).
2. Use a larger spaCy model or a custom one if accuracy matters, and measure it on your own articles.
3. Replace multiprocessing queues with a broker such as Redis or Kafka to scale across machines.

## Risks and limits

- The small spaCy model misses and mislabels entities, so counts are indicative.
- Entity counts say nothing about tone or context.
- Copyright limits apply to storing full article text.

**Case strength:** **Moderate.** The design is a good, clear ingest pattern. The ROI depends heavily on the assumed manual alternative.

## Sources

- spaCy, English models: https://spacy.io/models/en
