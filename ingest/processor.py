#########################################################################################################################
''' 
This module enables us to process text using NLP techniques to extract known entities.

It uses Spacy to extract entities based on a pre-built model. The model must be downloaded before using Spacy
'''
#########################################################################################################################

from collections import Counter
from typing import Dict

import spacy

from ingest.debugging import app_logger as log
from .models import Post, ProcessedPost


class DataProcessor():

    def __init__(self, model: str = 'en_core_web_sm'):
        log.info('spacy: loading model')
        # Only NER is needed; skipping the other pipes makes loading and processing faster.
        self.nlp = spacy.load(model, disable=['parser', 'lemmatizer'])
        log.info('spacy: loaded model')
        self.skip = ['CARDINAL', 'DATE', 'TIME', 'MONEY', 'ORDINAL']

    def entities(self, doc) -> Counter:
        t = [e.text.lower() for e in doc.ents if e.label_ not in self.skip]
        return Counter(t)

    def process(self, text: str) -> Dict:
        return {'entities': self.entities(self.nlp(text))}

    def process_message(self, post: Post) -> ProcessedPost:
        return ProcessedPost(
            **{
                **post.model_dump(include={'publication'}),
                'post_ids': [post.id] if post.id else [],
                **self.process(post.content)
            }
        )