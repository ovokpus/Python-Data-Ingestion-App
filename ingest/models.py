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
    publication: str = None # not required on creation
    entities: Counter = Counter()
    article_count: int = 0

    @property
    def pub_key(self) -> str:
        return self.publication.strip().lower()

    def _transform_for_database(self, top_n=2000) -> List[Tuple[str, str, str, Dict]]:
        # Return the top n entities
        for word, count in self.entities.most_common(top_n):
            yield self.pub_key, 'ent', str(hash(word)), {'word': word, 'count': count}
        yield self.pub_key, None, None, {'count': self.article_count}
        
    def transform_for_database(self, top_n=2000) -> List[Tuple[str, str, str, Dict]]:
        '''Returns a list of tuples containing one of two types of message.
        
        For messages used as Firestore documents storing the word and count:
        (publication, collection, doc_id, document_dict)
        
        For messages used to increment the publication's document count:
        publication, None, None, {'count': self.article_count}
        When consuming this type of message check the collection or doc_id for None values
        
        '''
        return list(self._transform_for_database(top_n))

    def __add__(self, other):
        self.article_count += 1
        self.publication = other.publication
        self.entities += other.entities
        return self
