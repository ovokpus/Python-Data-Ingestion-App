################################################################################################################
""" 
This module provides methods for persisting processed data to long-term storage.
"""
################################################################################################################

from google.cloud import firestore

from .debugging import app_logger as log
from .models import ProcessedPost


def persist_no_op(*args, **kwargs):
    pass


def get_database_client() -> firestore.Client:
    return firestore.Client()


def persist(client, pubname, collname, doc_id, document_dict):
    # Check these values to determine if this is a Publication count incrementor message
    if collname is None or doc_id is None:
        increment_publication(client, pubname, document_dict['count'])
    else:
        # Map the increment class to the count value.
        document_dict['count'] = firestore.Increment(document_dict['count'])
        # pubdoc is the firestore document for the given publication
        pubdoc = client.collection(u'publications').document(pubname)
        # wrddoc is the document that stores the word count for the given word
        wrddoc = pubdoc.collection(collname).document(doc_id)
        # Merge will update the count value for the given word
        wrddoc.set(document_dict, merge=True)
        log.debug('incremented word count for %s', doc_id)


def increment_publication(client, pubname, count):
    # pubdoc is the firestore document for the given publication
    pubdoc = client.collection(u'publications').document(pubname)
    # Merge will update the count value for the given word
    pubdoc.set({'count': firestore.Increment(count)}, merge=True)
    log.debug('incremented publication count for %s', pubname)
