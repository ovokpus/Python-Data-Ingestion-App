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

    def __init__(self):
        log.info('spacy: loading model')
        self.nlp = spacy.load('en_core_web_sm')
        log.info('spacy: loaded model')
        self.skip = ['CARDINAL', 'DATE', 'TIME', 'MONEY', 'ORDINAL']

    def entities(self, doc) -> Counter:
        t = [e.text.lower() for e in doc.ents if e.label not in self.skip]
        return Counter(t)

    def process(self, text: str) -> Dict:
        return {'entities': self.entities(self.nlp(text))}

    def process_message(self, post: Post) -> ProcessedPost:
        return ProcessedPost(
            **{
                **post.dict(),
                **self.process(post.content)
            }
        )