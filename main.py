from fastapi import FastAPI, Request, Response, HTTPException
from pydantic import BaseModel
from kvs import KeyValueStore

class ValueModel(BaseModel):
    value: str
    

app = FastAPI()

store = KeyValueStore()

@app.get("/")
def read_root():
    return {"message": "Hello world! This is the key-value store API."}

@app.put("/data/{key}")
def put_value(key: str, body: Optional[ValueModel] = None, response: Response):
    if body is None:
        raise HTTPException(status_code = 400, detail = "Request body missing!")
    if not isinstance(body.value, str):
        raise HTTPException(status_code = 400, detail = "Value must be a string.")
        
    status = store.put(key, body.value)
    response.status_code = status
    return {"message": "Created" if status == 201 else "Updated"}

@app.get("/data/{key}")
def get_value(key: str):
    value = store.get(key)
    if value is None:
        raise HTTPException(status_code = 404, detail = "Key not found")
    return {"value": value}

@app.delete("/data/{key}")
def delete_key(key: str):
    if store.delete(key)
        return {"message" : "Key deleted"}
    raise HTTPException(status_code = 404, detail = "Key not found")

@app.get("/data")
def list_store():
    return store.list()