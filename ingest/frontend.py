##########################################################################################################################
'''
This module provides a web based API for consuming Post data. it uses an API key for authorization.
'''
##########################################################################################################################

from fastapi import Depends, FastAPI, HTTPException, Security, status
from fastapi.security import APIKeyHeader

from .debugging import app_logger as log
from .messageq import QueueWrapper, create_queue_manager, regsister_manager
from .models import Post

# Use an access token to secure the post/enqueue API
API_KEY_HEADER = APIKeyHeader(name='access_token', auto_error=False)
app = FastAPI()

class Connector:
    '''
    Connector is used to manage the connection to the input queue.
    The queue manager requires a call to .connect() to establish the connection.
    By reusing connectionc we can better manage our netwoking resources.
    '''
    
    def __init__(self):
        pass
    
    def __call__(self):
        '''
        returns a connected input queue manager or raises an exception if the connection fails.
        '''
        
iqueue = Connector()

def check_auth_header(api_key_header: str = Security(API_KEY_HEADER)):
    pass