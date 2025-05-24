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

key_locks = defaultdict(asyncio.Lock) #for concurrent puts

def dep_check(deps: dict, client_md: dict) -> bool:
    for key, versions in deps.items():
        if key not in client_md:
            return False
        for version in versions:
            if version not in client_md[key]:
                return False
    return True

def arbitration_order(local: dict, foreign: dict) -> bool:
    if local["timestamp"] < foreign["timestamp"]:
        return True
    if local["timestamp"] == foreign["timestamp"]:
        return local["node"] < foreign["node"]
    return False

async def converge_nodes():
    async with httpx.AsyncClient() as client:
        for node in view:
            if node["id"] == nodeID:
                continue
            await client.post(
                f"http://{node['address']}/internal/converge", 
                json={"kvs": kvs}
            )

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
        #print(f"PUT {key=} {version=} {client_md=}")
        kvs[key] = {
            "value": value,
            "timestamp": timestamp,
            "node": nodeID,
            "version": version,
            "deps": dict(client_md)
        }
        #print(f"STORED {key=} {kvs[key]}")

    client_md.setdefault(key, []).append(version)
    await converge_nodes() #gossiping, maybe not even necessary
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
    #key_exists = key in client_md

    #print(f"{client_md}, {key_exists}")

    while True:
        if key in kvs:
            key_data = kvs[key]
            if dep_check(key_data["deps"], client_md):
                break
        else:
            if key not in client_md:
                raise HTTPException(status_code=404, detail="key doesn't exist")

        await asyncio.sleep(0.2)  # prevent CPU spin

    '''
    if key not in kvs:
        if key_exists:
            return JSONResponse(content={"message": "waiting on key"}, status_code=408)
        else:
            raise HTTPException(status_code=404, detail="key doesn't exist")
        
    key_data = kvs[key]
    if not dep_check(key_data["deps"], client_md):
        return JSONResponse(content={"message": "dependencies not satisfied"}, status_code=408)
    '''

    client_md.setdefault(key, [])
    #add version to md
    if key_data["version"] not in client_md[key]:
        client_md[key].append(key_data["version"])
    #add dependencies to md
    for key, versions in key_data["deps"].items():
        client_md.setdefault(key, [])
        for v in versions:
            if v not in client_md[key]:
                client_md[key].append(v)

    #print(f"client-md: {client_md}")

    return JSONResponse(content={"value": key_data["value"], "causal-metadata": client_md}, status_code=200)

@server.get("/data")
async def getAllKeys():
    if not view:
        return JSONResponse(content={"message": "node not online"}, status_code=503)
    
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="json body missing from request")
    
    if "causal-metadata" not in data:
        raise HTTPException(status_code=400, detail="'causal-metadata' is missing from request body")
    
    client_md = dict(data["causal-metadata"])
    items = {}

    for key in kvs.keys():
        while True:
            data = kvs[key]
            if dep_check(data["deps"], client_md):
                break
            elif key not in client_md:
                raise HTTPException(status_code=404, detail="key doesn't exist")

            await asyncio.sleep(0.2)  # prevent CPU spin

        client_md.setdefault(key, [])
        #add version to md
        if data["version"] not in client_md[key]:
            client_md[key].append(data["version"])
        #add dependencies to md
        for dep_key, versions in data["deps"].items():
            client_md.setdefault(dep_key, [])
            for v in versions:
                if v not in client_md[dep_key]:
                    client_md[dep_key].append(v)

        items[key] = data["value"]

    return JSONResponse(content={"items": items, "causal-metadata": client_md}, status_code=200)

@server.put("/view")
async def putView(request: Request):
    global view
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="json body missing from request")
    
    if "view" not in data:
        raise HTTPException(status_code=400, detail="'view' is missing from request body")
    
    view = data["view"]
    if not any(node["id"] == nodeID for node in view):
        view = []
        return JSONResponse(content={"message": "new view accepted"}, status_code=200) 

    #converge data of all nodes in view
    await converge_nodes()
    await asyncio.sleep(0.2)
    await converge_nodes()

    return JSONResponse(content={"message": "new view accepted"}, status_code=200)

@server.post("/internal/converge")
async def converge(request: Request):
    data = await request.json()
    foreign_kvs = data["kvs"]

    for key, data in foreign_kvs.items():
        async with key_locks[key]:
            if key in kvs:
                #if versions match up go to next key
                if data["version"] == kvs[key]["version"]:
                    continue

                #if incoming key causally depends on version present then just accept it
                if dep_check(data["deps"], {key: kvs[key]["version"]}):
                    kvs[key] = dict(data)
                elif not dep_check(data["deps"], {key: kvs[key]["version"]}) and not dep_check(kvs[key]["deps"], {key: [data["version"]]}):
                    #if versions are concurrent check the arbitration order
                    if arbitration_order(kvs[key], data):
                        kvs[key] = dict(data)
                #if logic reaches here, incoming version is an old version so ignore it
            else:
                kvs[key] = dict(data)

    return JSONResponse(content={"message": "convergence done"}, status_code=200)


if __name__ == '__main__':
    server.run(host='0.0.0.0', port=8081)