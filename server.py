from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from collections import defaultdict
import httpx
import asyncio
import os
import time

server = FastAPI()

kvs = {}
view = []
nodeID = int(os.environ.get("NODE_IDENTIFIER", -1)) #every server should have a diff one
node_md = {}

key_locks = defaultdict(asyncio.Lock) #for concurrent puts

def dep_check(deps: dict, client_md: dict) -> bool:
    for key, versions in deps.items():
        if key not in client_md:
            return False
        for version in versions:
            if version not in client_md[key]:
                return False
    return True

@server.get("/")
async def hello():
    return "this is cse138 assignment3!"

@server.get("/ping")
def ping():
    return JSONResponse(content={"message": "node ready"}, status_code=200)

@server.put("/data/{key}")
async def putKey(key: str, request: Request):
    if not view:
        return JSONResponse(content={"message": "node not online"}, status_code=503)

    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="json body missing from request")
    
    if "value" not in data:
        raise HTTPException(status_code=400, detail="'value' is missing from request body")
    if "causal-metadata" not in data:
        raise HTTPException(status_code=400, detail="'causal-metadata' is missing from request body")
    
    value = data["value"]
    client_md = dict(data["causal-metadata"])

    async with key_locks[key]:
        timestamp = time.time()
        version = f"{nodeID}_{timestamp}"
        kvs[key] = {
            "value": value,
            "timestamp": timestamp,
            "node": nodeID,
            "version": version,
            "deps": dict(client_md)
        }

    node_md.setdefault(key, []).append(version)
    client_md.setdefault(key, []).append(version)
    return JSONResponse(content={"causal-metadata": client_md}, status_code=200)
    
@server.get("/data/{key}")
async def getKey(key: str, request: Request):
    if not view:
        return JSONResponse(content={"message": "node not online"}, status_code=503)
    
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="json body missing from request")
    
    if "causal-metadata" not in data:
        raise HTTPException(status_code=400, detail="'causal-metadata' is missing from request body")
    
    client_md = dict(data["causal-metadata"])
    key_exists = key in client_md

    if key not in kvs:
        if key_exists:
            return JSONResponse(content={"message": "waiting on key"}, status_code=503)
        else:
            raise HTTPException(status_code=404, detail="key doesn't exist")
        
    key_data = kvs[key]
    if not dep_check(key_data["deps"], client_md):
        return JSONResponse(content={"message": "dependencies not satisfied"}, status_code=503)

    client_md.setdefault(key, [])
    if key_data["version"] not in client_md[key]:
        client_md[key].append(key_data["version"])

    return JSONResponse(content={"value": key_data["value"], "causal-metadata": client_md}, status_code=200)

@server.get("/data")
def getAllKeys():
    return JSONResponse(content=kvs, status_code=200)

@server.put("/view")
async def putView(request: Request):
    global view, primaryID, primaryAddress, primaryIP
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="json body missing from request")
    
    if "view" not in data:
        raise HTTPException(status_code=400, detail="'view' is missing from request body")
    
    view = data["view"]
    #converge data of all nodes in view

    return JSONResponse(content={"message": "new view accepted"}, status_code=200)

if __name__ == '__main__':
    server.run(host='0.0.0.0', port=8081)