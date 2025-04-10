from fastapi import FastAPI, Request
from kvs import KeyValueStore

app = FastAPI()

store = KeyValueStore()

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.put("/data/{key}")
# Body must be a json object with field "value" set to the string to store at key
# Key is a string that is valid for URLs
# Return 201 if new, 200 if update
# Return 400 if request body is missing
def put_value(key: str, request: Request) -> str:
    try:
        data = request.json()
    except Exception:
        return {"status":400}
    
    # Make sure value is valid for URL
    
    status = store.put(key, data["value"])
    return {"status": status}

@app.get("/data/{key}")
# Return json obj, "value" field assoc w/ key, and status 200
# If no key, return 404
def get_value(key: str):
    value = store.get(key)
    if value is None:
        return {"response": "404"}
    return {"value": value, "status": 200}

@app.delete("/data/{key}")
def delete_key(key: str):
    key_existed = store.delete(key)
    
    if key_existed:
        return {"status": 200}
    else:
        return {"status": 404}

@app.get("/data")
def list_store():
    return store.list()