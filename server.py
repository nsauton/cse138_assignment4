from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from collections import defaultdict
import httpx
import asyncio
import os

server = FastAPI()

kvs = {}
view = []
nodeID = int(os.environ.get("NODE_IDENTIFIER", -1)) #every server should have a diff one, highest will be the primary
primaryID = -1
primaryAddress = ""
primaryIP = ""

key_locks = defaultdict(asyncio.Lock) #for concurrent puts

async def replicateRequest(method: str, key:str, body:dict = None):
    requests = []
    async with httpx.AsyncClient() as client:
        for node in view:
            if node["id"] == nodeID:
                continue

            address = node["address"]
            route = f"http://{address}/internal/replicate/{key}"
            requests.append(client.request(method, route, json=body))

        await asyncio.gather(*requests, return_exceptions=True)

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
    
    if nodeID != primaryID and request.client.host != primaryIP:
         async with httpx.AsyncClient() as client:
            try: 
                response = await client.put(
                    f"http://{primaryAddress}/data/{key}",
                    content=await request.body(),
                    headers={"Content-Type": request.headers.get("Content-Type")},
                    timeout=2.0
                )
                return JSONResponse(content=response.json(), status_code=response.status_code)
            except httpx.RequestError:
                await asyncio.sleep(3600) #let hang infinitely
                return

    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="json body missing from request")
    
    if "value" not in data:
        raise HTTPException(status_code=400, detail="'value' is missing from request body")
    
    value = data["value"]
    keyExists = key in kvs

    async with key_locks[key]:
        kvs[key] = value
        if nodeID == primaryID:
            await replicateRequest("PUT", key, {"value": value})

    return JSONResponse(content={key: value}, status_code=200 if keyExists else 201)
    
@server.get("/data/{key}")
def getKey(key: str):
    if not view:
        return JSONResponse(content={"message": "node not online"}, status_code=503)
    
    if key in kvs:
        return JSONResponse(content={"value": kvs[key]}, status_code=200)
    else:
        raise HTTPException(status_code=404, detail="key doesn't exist")
    
@server.delete("/data/{key}")
async def deleteKey(key: str, request: Request):
    if not view:
        return JSONResponse(content={"message": "node not online"}, status_code=503)
    
    if nodeID != primaryID and request.client.host != primaryIP:
        async with httpx.AsyncClient() as client:
            try:
                response = await client.delete(
                    f"http://{primaryAddress}/data/{key}",
                    content=await request.body(),
                    headers={"Content-Type": request.headers.get("Content-Type")},
                    timeout=2.0
                )
                return JSONResponse(content=response.json(), status_code=response.status_code)
            except httpx.RequestError:
                await asyncio.sleep(3600) #let hang infinetly
                return
    
    if key in kvs:
        value = kvs.pop(key)
        if nodeID == primaryID:
            await replicateRequest("DELETE", key)
        return JSONResponse(content={key: value}, status_code=200)
    else:
        raise HTTPException(status_code=404, detail="key doesn't exist")

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
    primaryNode = max(view, key=lambda node: node["id"])
    primaryID = primaryNode["id"]
    primaryAddress = primaryNode["address"]
    primaryIP = primaryNode["address"].split(":")[0]

    #sync data to all node in view?

    return JSONResponse(content={"message": "new view accepted"}, status_code=200)

@server.put("/internal/replicate/{key}")
async def internalReplicatePut(key: str, request: Request):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="json body missing from replication request")

    if "value" not in data:
        raise HTTPException(status_code=400, detail="'value' is missing from replication body")

    keyExists = key in kvs
    kvs[key] = data["value"]
    return JSONResponse(content={"message": "Replicated PUT successful"}, status_code=200 if keyExists else 201)

@server.delete("/internal/replicate/{key}")
async def internalReplicateDelete(key: str):
    if key in kvs:
        kvs.pop(key)
    return JSONResponse(content={"message": "Replicated DELETE successful"}, status_code=200)

if __name__ == '__main__':
    server.run(host='0.0.0.0', port=8081)