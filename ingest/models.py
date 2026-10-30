###################################################################################################################
'''
This module provides models used as messages to be passed via message queues.
'''
###################################################################################################################

from typing import Counter, Dict, Iterator, List, Optional, Tuple

from pydantic import BaseModel, Field

FLUSH = '_flush'


class Post(BaseModel):
    '''
    A post is used to store content and publication from the front-end
    '''
    content: str = Field(min_length=1)  # required
    publication: str = Field(min_length=1)  # required
    title: Optional[str] = None
    id: Optional[str] = None  # assigned by the front-end so the post can be tracked through the pipeline


class ProcessedPost(BaseModel):
    '''
    A processed post is used to storethe results of the DataProcessor
    '''
    publication: Optional[str] = None  # not required on creation
    entities: Counter[str] = Field(default_factory=Counter)
    article_count: int = 0
    post_ids: List[str] = Field(default_factory=list)

    @property
    def pub_key(self) -> str:
        return self.publication.strip().lower()

    def _transform_for_database(self, top_n=2000, worker_pid: int = None) -> Iterator[Tuple[str, str, str, Dict]]:
        # Return the top n entities. The word itself is the key: the original used hash(word), which is
        # randomised per process, so two workers wrote the same word under different ids.
        for word, count in self.entities.most_common(top_n):
            yield self.pub_key, 'ent', word, {'word': word, 'count': count}
        yield self.pub_key, None, None, {'count': self.article_count}
        yield self.pub_key, FLUSH, None, {'post_ids': self.post_ids, 'posts': self.article_count,
                                          'entities': len(self.entities), 'worker_pid': worker_pid,
                                          'publication': self.publication}

    def transform_for_database(self, top_n=2000, worker_pid: int = None) -> List[Tuple[str, str, str, Dict]]:
        '''Returns a list of tuples containing one of three types of message.

        Word counts: (publication, 'ent', word, {'word': word, 'count': n})
        Publication article count: (publication, None, None, {'count': n})
        Flush record: (publication, '_flush', None, {'post_ids': [...], 'posts': n, 'entities': n, ...})
        '''
        return list(self._transform_for_database(top_n, worker_pid))

    def __add__(self, other):
        self.article_count += 1
        self.publication = other.publication
        self.entities += other.entities
        self.post_ids += other.post_ids
        return self
