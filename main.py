from fastapi import FastAPI, Request, Response, HTTPException, Body, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from kvs import KeyValueStore
from node import Node
from typing import Optional
import os

# Initialize the Node ID
node = Node( int(os.getenv("NODE_IDENTIFIER")) )

class ValueModel(BaseModel):
    value: str
    

app = FastAPI()

# JANK Solution. Overriding the default validation error 422 with 400.
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": exc.errors()}
    )


store = KeyValueStore()

@app.get("/")
def read_root():
    return {"message": "Hello world! This is the key-value store API."}

@app.get("/ping")
def ping():
    return {"message": "ping!", "node_id": f"{node.NODE_IDENTIFIER}"}

@app.put("/data/{key}")
def put_value(key: str, response: Response, body: Optional[dict] = Body(default=None)):

    if node.NODE_IDENTIFIER not in node.view:
        raise HTTPException(status_code=503, detail="Node is not online yet!")
    
    if node.NODE_IDENTIFIER != node.primary:
        # Change Status Code Later
        raise HTTPException(status_code=503, detail="Node is not a primary!")
    
    if body is None:
        raise HTTPException(status_code=400, detail="Request body missing!")

    value = body.get("value")
    if not isinstance(value, str):
        raise HTTPException(status_code=400, detail="Field 'value' must be a string.")
    
    # Communicate with the backups in view!

    status = store.put(key, value)
    response.status_code = status
    return {"message": "Created" if status == 201 else "Updated"}

@app.get("/data/{key}")
def get_value(key: str):

    if node.NODE_IDENTIFIER not in node.view:
        raise HTTPException(status_code=503, detail="Node not online yet!")
        
    value = store.get(key)
    if value is None:
        raise HTTPException(status_code = 404, detail = "Key not found")
    return {"value": value}

@app.delete("/data/{key}")
def delete_key(key: str):

    if node.NODE_IDENTIFIER not in node.view:
        raise HTTPException(status_code=503, detail="Node not online yet!")
    
    if node.NODE_IDENTIFIER != node.primary:
        # Change Status Code Later
        raise HTTPException(status_code=503, detail="Node is not a primary!")
    
    # Communicate with the backups in view!

    if store.delete(key):
        return {"message" : "Key deleted"}
    raise HTTPException(status_code = 404, detail = "Key not found")

@app.get("/data")
def list_store():
    
    if node.NODE_IDENTIFIER not in view:
        raise HTTPException(status_code=503, detail="Node not online yet!")
    
    return store.list()

@app.put("/view")
def view(body: Optional[dict] = Body(default=None)):

    # Store Requested View
    node.view = {x["id"]:x["address"] for x in body["view"]}

    # Decide a Primary based on smallest NODE_IDENTIFIER
    if not node.primary or node.primary not in node.view:
        node.primary = min(node.view)

    # Debugging
    if node.NODE_IDENTIFIER == node.primary:
        print("I'm the primary!")

    return {"message": "view!", "node_id": f"{node.NODE_IDENTIFIER}"}