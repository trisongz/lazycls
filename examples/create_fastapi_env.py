
"""
Super lazy way to create a new api

This is where the real magic happens.
Almost all the params are controlled via environment variables.

Check lazy/api/config.py for full configuration
"""

import os
os.environ.setdefault('APP_TITLE', 'LazyAPI Test')
os.environ.setdefault('APP_VERSION', '0.0.2')

from lazy.api import *

app = create_fastapi('testapp')

@app.get('/')
async def app_get():
    return 'hi'

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app)