from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from collections import defaultdict
import httpx
import asyncio
import os
import time
import random
import hashlib
import json

server = FastAPI()

kvs = {}
view = {}
nodeID = int(os.environ.get("NODE_IDENTIFIER", -1)) #every server should have a diff one
key_locks = defaultdict(asyncio.Lock) #for concurrent puts
shard_nodes = []
shard_name = ""

# --- Helpful Functions --- #
def dep_check(deps: dict, client_md: dict) -> bool:
    for dep_key, dep_version in deps.items():
        if dep_key in client_md:
            continue
        else:
            return False
    return True

def arbitration_order(local: dict, foreign: dict) -> bool:
    if local["timestamp"] < foreign["timestamp"]:
        return True
    if local["timestamp"] == foreign["timestamp"]:
        return local["node"] < foreign["node"]
    return False

def find_correct_shard(key: str) -> str:
    shard_names = sorted(view.keys())
    hash = int(hashlib.sha1(key.encode()).hexdigest(), 16)
    return shard_names[hash % len(shard_names)]
# --- Helpful Functions --- #

# --- Async functions --- #
async def send_key_to_shard(key: str, entry: dict, nodes: list[dict]):
    tasks = []
    async with httpx.AsyncClient() as client:
        for node in nodes:
            tasks.append(client.post(
                f"http://{node['address']}/internal/acceptKey",
                json={
                    "key": key, 
                    "value": entry["value"],
                    "version": entry["version"],
                    "deps": entry["deps"]
                }
            ))
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            print(f"failed to send key: {e}")


async def converge_nodes(max_nodes: int):
    gossips = []
    #just gonna converge with all nodes in the shard for now
    #chosen = random.sample([n for n in view if n["id"] != nodeID], min(max_nodes, len(view)-1))

    async with httpx.AsyncClient() as client:

        for node in shard_nodes:
            gossips.append(client.post(
                f"http://{node['address']}/internal/converge", 
                json={"kvs": kvs}
            ))
        try:
            await asyncio.gather(*gossips)
        except Exception as e:
            print(f"Gossip error: {e}")

async def background_gossip():
    while True:
        await asyncio.sleep(2)
        if view:
            #print("Gossip time!")
            await converge_nodes(2)


# --- Gossip Protocol --- #

@server.get("/")
async def hello():
    return "this is cse138 assignment3!"

@server.get("/ping")
def ping():
    return JSONResponse(content={"message": "node ready"}, status_code=200)

@server.put("/data/{key}")
async def putKey(key: str, request: Request):

    # --- HTTP Error Handling --- #
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
    # --- HTTP Error Handling --- #

    # check if key belongs in this shard else proxy it to correct shard, just use put
    correct_shard = find_correct_shard(key)
    if correct_shard != shard_name:
        print(f"wrong shard sending to sorrect shard: {correct_shard}")
        node = random.choice(view[correct_shard])
        async with httpx.AsyncClient() as client:
            try:
                res = await client.put(
                    f"http://{node["address"]}/data/{key}",
                    json=data
                )
                return JSONResponse(content=res.json(), status_code=res.status_code)
            except Exception as e:
                raise HTTPException(status_code=503, detail="forwarding failed")
    
    # Store client's request such as "value" and "causal-metadata"
    value = data["value"]
    client_md = dict(data["causal-metadata"])

    async with key_locks[key]:
        version = {"timestamp": time.time(), "node": nodeID} # ALWAYS update version
        kvs[key] = {
            "value": value, # As usual, store value
            "version": version, # Store timestamp and node ID
            "deps": dict(client_md) # Dependencies, will help with causal consistency
        }
        client_md[key] = version # Update causal-metadata to prepare for gossiping...
    await converge_nodes(2) #gossiping, maybe not even necessary
    return JSONResponse(content={"causal-metadata": client_md}, status_code=200)
    
@server.get("/data/{key}")
async def getKey(key: str, request: Request):
    if not view:
        return JSONResponse(content={"message": "node not online"}, status_code=503)
    
    #getting md from header
    client_md = {}
    header_md = request.headers.get("X-Causal-Metadata")
    if header_md:
        client_md = json.loads(header_md)
    else:
     # --- HTTP Error Handling --- #
        try:
            data = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="json body missing from request")
        
        if "causal-metadata" not in data:
            raise HTTPException(status_code=400, detail="'causal-metadata' is missing from request body")
        
        # Store client's request such as "causal-metadata"
        client_md = dict(data["causal-metadata"])
     # --- HTTP Error Handling --- #

    #check if key belongs in this shard else proxy it to correct shard, just use get
    correct_shard = find_correct_shard(key)
    if correct_shard != shard_name:
        print(f"wrong shard sending to sorrect shard: {correct_shard}")
        node = random.choice(view[correct_shard])
        async with httpx.AsyncClient() as client:
            try:
                res = await client.get(
                    f"http://{node["address"]}/data/{key}",
                    headers={"X-Causal-Metadata": json.dumps(client_md)}
                )
                return JSONResponse(content=res.json(), status_code=res.status_code)
            except Exception as e:
                raise HTTPException(status_code=503, detail="forwarding failed")

    while True:
        # If the client_md is empty, (NO OPERATIONS SEEN)
            # If it exist in KVS, update client_md and return value.
            # If it doesn't exist in KVS, return 404.
        if client_md == {}:
            if key in kvs:
                key_data = kvs[key]
                client_md[key] = key_data["version"]
                break
            else:
                raise HTTPException(status_code=404, detail="key doesn't exist")

        # If the client_md is not empty, (OPERATIONS SEEN)
        else:
            if key in kvs:
                key_data = kvs[key]
                key_deps = key_data["deps"]
                key_version = key_data["version"]
                #print(f"[GET] client_md: {client_md},\n key: {key_data}")
                if (dep_check(key_deps, client_md)): # Check if the client seen these dependencies
                    # Check if client has seen this key before
                    if key in client_md:
                        client_version = client_md[key]
                        if client_version["timestamp"] <= key_version["timestamp"]: # we can't read old versions of the key, only newer ones
                            for dep_key, dep_version in key_deps.items():
                                if dep_key in client_md:
                                    if arbitration_order(client_md[dep_key], dep_version):
                                        client_md[dep_key] = dict(dep_version)      
                            break
                    else: # We can accept if it doesn't contain in client_md
                        for dep_key, dep_version in key_deps.items():
                            if dep_key in client_md:
                                if arbitration_order(client_md[dep_key], dep_version):
                                    client_md[dep_key] = dict(dep_version)    
                        break
            await asyncio.sleep(0.2) # Hang indefinitely till the key exist in KVS

    client_md[key] = key_data["version"]

    return JSONResponse(content={"value": key_data["value"], "causal-metadata": client_md}, status_code=200)

@server.get("/data")
async def getAllKeys(request: Request):
    
    # --- HTTP Error Handling --- #
    if not view:
        return JSONResponse(content={"message": "node not online"}, status_code=503)
    
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="json body missing from request")
    
    if "causal-metadata" not in data:
        raise HTTPException(status_code=400, detail="'causal-metadata' is missing from request body")
    # --- HTTP Error Handling --- #
    
    client_md = dict(data["causal-metadata"])
    initial_md = dict(client_md)
    items = {}

    #get all keys in this shard

    # Create an union of keys (KVS's keys U Client's Seen Operations)
    keys_set = set(kvs.keys()).union(set(client_md.keys()))

    for key in keys_set:
        while True:
            # If the client_md is empty, (NO OPERATIONS SEEN)
                # If it exist in KVS, update save value but dont update metadata till later
                # If it doesn't exist in KVS, return 404.
            if initial_md == {}:
                if key in kvs:
                    key_data = kvs[key]
                    break
                else:
                    raise HTTPException(status_code=404, detail="key doesn't exist")

            # If the client_md is not empty, (OPERATIONS SEEN)
            else:
                if key in kvs:
                    key_data = kvs[key]
                    key_deps = key_data["deps"]
                    key_version = key_data["version"]
                    #print(f"[GET] client_md: {client_md},\n key: {key_data}")
                    if (dep_check(key_deps, initial_md)): # Check if the client seen these dependencies
                        # Check if client has seen this key before
                        if key in initial_md:
                            client_version = initial_md[key]
                            if client_version["timestamp"] <= key_version["timestamp"]: # we can't read old versions of the key, only newer ones
                                for dep_key, dep_version in key_deps.items():
                                    if dep_key in initial_md:
                                        if arbitration_order(initial_md[dep_key], dep_version):
                                            client_md[dep_key] = dict(dep_version)      
                                break
                        else: # We can accept if it doesn't contain in client_md
                            for dep_key, dep_version in key_deps.items():
                                if dep_key in initial_md:
                                    if arbitration_order(initial_md[dep_key], dep_version):
                                        client_md[dep_key] = dict(dep_version)    
                            break
                await asyncio.sleep(0.2) # Hang indefinitely till the key exist in KVS

        #update items with key and client md if it was not empty to start
        items[key] = key_data["value"]
        client_md[key] = key_data["version"]

    '''
    #finally update metadata for clients who came in with empty md
    if client_md == {}:
        for key in items:
            client_md[key] = kvs[key]["version"]
    '''

    #print(items)
    return JSONResponse(content={"items": items, "causal-metadata": client_md}, status_code=200)


@server.put("/view")
async def putView(request: Request):
    global view, shard_nodes, shard_name
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="json body missing from request")
    
    view = data["view"]
    shard_name = ""
    shard_nodes = []
    for name, nodes in data["view"].items():
        #print(f"name: {name}, nodes: {nodes}")
        if any(node["id"] == nodeID for node in nodes):
            shard_nodes = nodes
            shard_name = name
    
    print(f"name: {shard_name}, nodes: {shard_nodes}")
    
    #transfer key info to correct shard
    bad_keys = []
    for key, entry in kvs.items():
        correct_shard = find_correct_shard(key)
        print(correct_shard)
        if correct_shard != shard_name:
            await send_key_to_shard(key, entry, view[correct_shard])
            bad_keys.append(key)

    for key in bad_keys:
        del kvs[key]

    #this may no longer be needed
    '''
    #converge data of all nodes in view
    await converge_nodes(len(view))
    await asyncio.sleep(0.2)
    await converge_nodes(len(view))
    '''

    if not shard_name:
        view = {}

    return JSONResponse(content={"message": "new view accepted"}, status_code=200)

#Post helper routes

@server.post("/internal/acceptKey")
async def acceptKey(request: Request):
    data = await request.json()
    key = data["key"]
    entry = {
        "value": data["value"],
        "version": data["version"], 
        "deps": data["deps"],
    }

    async with key_locks[key]:
        kvs[key] = entry

    return JSONResponse(content={"message": "sent key accpeted"}, status_code=200)

@server.post("/internal/converge")
async def converge(request: Request):
    data = await request.json()
    foreign_kvs = data["kvs"]

    for key, data in foreign_kvs.items():
        async with key_locks[key]:
            #print(f"Local KVS: {kvs}\nForeign KVS: {foreign_kvs}")
            if key in kvs:
                # If the version is the same, it's okay
                if data["version"] == kvs[key]["version"]:
                    continue
                # We want the most recent version of the key
                if arbitration_order(kvs[key]["version"], data["version"]):
                    kvs[key] = dict(data)
                    kvs[key]["deps"] = dict()
            else:
                # If local KVS doesn't contain the key, ADD IT
                kvs[key] = dict(data)
                kvs[key]["deps"] = dict()

    return JSONResponse(content={"message": "convergence done"}, status_code=200)

@server.on_event("startup")
async def startup_event():
    asyncio.create_task(background_gossip())


if __name__ == '__main__':
    server.run(host='0.0.0.0', port=8081)