from fastapi import FastAPI, Request, Response, HTTPException, Body, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from kvs import KeyValueStore
from node import Node
from typing import Optional
import httpx
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
async def read_root():
    return {"message": "Hello world! This is the key-value store API."}

@app.get("/ping")
async def ping():
    return {"message": "ping!", "node_id": f"{node.NODE_IDENTIFIER}"}

@app.put("/data/{key}")
async def put_value(key: str, response: Response, body: Optional[dict] = Body(default=None)):

    if node.NODE_IDENTIFIER not in node.view:
        raise HTTPException(status_code=503, detail="Node is not online yet!")
    
    # Contact Primary if a write request was received.
    if node.NODE_IDENTIFIER != node.primary:
        if node.primary != None and node.primary in node.view:
            try:
                async with httpx.AsyncClient() as client:
                    r = await client.put(f"http://{node.view[node.primary]}/data/{key}", json={"value": body.get("value")})
                    response.status_code = r.status_code # Be sure to recover the status code as we went from BACKUP -> PRIMARY -> BACKUP
                    return r.json()
            except httpx.RequestError:
                raise HTTPException(status_code=408, detail="Timedout!") 
        else:
            raise HTTPException(status_code=503, detail="Primary Not Available!")
    
    if body is None:
        raise HTTPException(status_code=400, detail="Request body missing!")

    value = body.get("value")
    if not isinstance(value, str):
        raise HTTPException(status_code=400, detail="Field 'value' must be a string.")
    
    # Communicate with the backups in view!
    for node_id in node.view:
        if node.NODE_IDENTIFIER != node_id:
            try:
                async with httpx.AsyncClient() as client:
                    r = await client.put(f"http://{node.view[node_id]}/communication/{key}", json={"value": value})
            except httpx.RequestError: # Send a timeout response if failure
                raise HTTPException(status_code=408, detail="Timedout!") 
                
    status = store.put(key, value)
    response.status_code = status
    return {"message": "Created" if status == 201 else "Updated"}

@app.get("/data/{key}")
async def get_value(key: str):

    if node.NODE_IDENTIFIER not in node.view:
        raise HTTPException(status_code=503, detail="Node not online yet!")
        
    value = store.get(key)
    if value is None:
        raise HTTPException(status_code = 404, detail = "Key not found")
    return {"value": value}

@app.delete("/data/{key}")
async def delete_key(key: str):

    if node.NODE_IDENTIFIER not in node.view:
        raise HTTPException(status_code=503, detail="Node not online yet!")
    
    # Contact Primary if a delete request was received.
    if node.NODE_IDENTIFIER != node.primary:
        if node.primary != None and node.primary in node.view:
            try:
                async with httpx.AsyncClient() as client:
                    r = await client.delete(f"http://{node.view[node.primary]}/data/{key}")
                    return r.json()
            except httpx.RequestError: # Send a timeout response if failure
                raise HTTPException(status_code=408, detail="Timedout!") 
        else:
            raise HTTPException(status_code=503, detail="Primary Not Available!")
    
    # Communicate with the backups in view!
    for node_id in node.view:
        if node.NODE_IDENTIFIER != node_id:
            try:
                async with httpx.AsyncClient() as client:
                    r = await client.delete(f"http://{node.view[node_id]}/communication/{key}")
            except httpx.RequestError: # Send a timeout response if failure
                raise HTTPException(status_code=408, detail="Timedout!") 

    if store.delete(key):
        return {"message" : "Key deleted"}
    raise HTTPException(status_code = 404, detail = "Key not found")

@app.get("/data")
async def list_store():
    
    if node.NODE_IDENTIFIER not in node.view:
        raise HTTPException(status_code=503, detail="Node not online yet!")
    
    return store.list()

@app.put("/view")
async def view(body: Optional[dict] = Body(default=None)):
    # Store the old view to compare with the new view
    old_view = set(node.view)
    
    # Store Requested View
    node.view = {x["id"]:x["address"] for x in body["view"]}

    # Decide a Primary based on smallest NODE_IDENTIFIER
    if not node.primary or node.primary not in node.view:
        node.primary = min(node.view)
    
    # If this is a backup node, forward the view to the primary
    if node.NODE_IDENTIFIER != node.primary and node.primary in node.view:
        try:
            async with httpx.AsyncClient() as client:
                await client.put(f"http://{node.view[node.primary]}/view", json=body)
        except httpx.RequestError as e:
            print(f"Failed to forward view to primary: {e}")
    
    # If this is the primary node, push the KVS data to new nodes
    if node.NODE_IDENTIFIER == node.primary:
        new_view = set(node.view)
        new_nodes = new_view - old_view
        
        for new_node_id in new_nodes:
            node_address = node.view[new_node_id]
            try:
                async with httpx.AsyncClient() as client:
                    # Loop through store and send all pairs to new node
                    for key, value in store.list().items():
                        await client.put(f"http://{node_address}/communication/{key}", json={"value": value}, timeout=10)
            except httpx.RequestError as e:
                print(f"Failed to push data to {node_address}: {e}")
    
    # Debugging
    if node.NODE_IDENTIFIER == node.primary:
        print("I'm the primary!")
    
    return {"message": "view!", "node_id": f"{node.NODE_IDENTIFIER}"}

# The acknowledgement process
@app.put("/communication/{key}")
async def communication(key: str, response: Response, body: Optional[dict] = Body(default=None)):

    value = body.get("value")

    if not isinstance(value, str):
        raise HTTPException(status_code=400, detail="Field 'value' must be a string.")
    
    status = store.put(key, value)
    response.status_code = status
    
    return {"message": "Backup saved successfully"}

@app.delete("/communication/{key}")
async def communication(key: str):

    if store.delete(key):
        return {"message" : "Backup Key deleted"}
    raise HTTPException(status_code = 404, detail = "Key not found")