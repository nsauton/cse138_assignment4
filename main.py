from fastapi import FastAPI, Request, Response, HTTPException, Body, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from kvs import KeyValueStore
from typing import Optional

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

@app.put("/data/{key}")
def put_value(key: str, response: Response, body: Optional[dict] = Body(default=None)):
    if body is None:
        raise HTTPException(status_code=400, detail="Request body missing!")

    value = body.get("value")
    if not isinstance(value, str):
        raise HTTPException(status_code=400, detail="Field 'value' must be a string.")

    status = store.put(key, value)
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
    if store.delete(key):
        return {"message" : "Key deleted"}
    raise HTTPException(status_code = 404, detail = "Key not found")

@app.get("/data")
def list_store():
    return store.list()