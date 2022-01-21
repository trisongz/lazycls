
"""
Super lazy way to create a new api
"""

from lazy.api import *

app = create_fastapi('testapp')

@app.get('/')
async def app_get():
    return 'hi'

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app)