###################################################################################################################
'''
This module provides models used as messages to be passed via message queues.
'''
###################################################################################################################

from collections import Counter
from typing import Any, Dict, List, Tuple

from pydantic import BaseModel


class Post(BaseModel):
    '''
    A post is used to store content and publication from the front-end
    '''
    content: str  # required
    publication: str  # required


class ProcessedPost(BaseModel):
    '''
    A processed post is used to storethe results of the DataProcessor
    '''
    publicastion: str
    entities: Counter = Counter()
    article_count: int = 0

    @property
    def pub_key(self) -> str:
        return None

    def transform_for_database(self, top_n=2000) -> List[Tuple[str, str, str, Dict]]:
        return None

    def __add__(self, other) -> ProcessedPost:
        return self
