# Key-Value Store

class KeyValueStore():
    def __init__(self):
        self.store = {}
    
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
        if key in self.store:
            del self.store[key]
            return True
        return False
    
    def list(self):
        return self.store