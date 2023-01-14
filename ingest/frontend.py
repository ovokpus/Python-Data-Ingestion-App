##########################################################################################################################
'''
This module provides a web based API for consuming Post data. it uses an API key for authorization.
'''
##########################################################################################################################

from fastapi import Depends, FastAPI, HTTPException, Security, status
from fastapi.security import APIKeyHeader

from .debugging import app_logger as log
from .messageq import QueueManager, QueueWrapper, create_queue_manager, register_manager
from .models import Post

# Use an access token to secure the post/enqueue API
API_KEY_HEADER = APIKeyHeader(name='access_token', auto_error=False)
app = FastAPI()

class Connector:
    '''
    Connector is used to manage the connection to the input queue.
    The queue manager requires a call to .connect() to establish the connection.
    By reusing connections we can better manage our netwoking resources.
    '''
    
    def __init__(self):
        register_manager('iqueue')
        self.manager = create_queue_manager(50000)
        self.queue = None
        
              
    
    def __call__(self):
        '''
        returns a connected input queue manager or raises an exception if the connection fails.
        '''
        if self.iqueue:
            return self.iqueue
        
        try:
            self.iqueue = self.manager.iqueue()
            return self.iqueue
        except AssertionError as ae:
            if ae.args == ('server not yet started',):
                try:
                    self.manager.connect()
                except ConnectionRefusedError:
                    raise
                
                return self()
        
        except Exception as ex:
            print(ex)
            raise
        
# iqueue = Connector()
iqueue = Connector()

def check_auth_header(api_key_header: str = Security(API_KEY_HEADER)):
    '''
    Check the API key header for a valid token.
    '''
    
    if api_key_header == 'password':
        return True
    
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Invalid API key"
    )

@app.post("/post/enqueue", status_code=status.HTTP_201_CREATED)
def create_post(post: Post, queue: QueueWrapper = Depends(iqueue), authenticated: bool = Depends(check_auth_header)):
    try:
        queue.put(post)
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f'Failed to enqueue post: {ex}')
    return ex