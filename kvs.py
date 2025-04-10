# Key-Value Store

class KeyValueStore():
    def __init__(self):
        self.store = {"test":"success"}
    
    def get(self, key: str):
        if key in self.store:
            return self.store[key]
        else:
            return None
    
    def put(self, key: str, value: str) -> int:
        if key in self.store:
            self.store[key] = value
            return 200
        else:
            self.store[key] = value
            return 201
    
    def delete(self, key: str) -> bool:
        res = self.store.pop(key, None) is not None
        return
    
    def list(self):
        return self.store