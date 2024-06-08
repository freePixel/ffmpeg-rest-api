import db
import os
import uuid
from functools import wraps
from flask import request, jsonify
def validApiKey(key: str):
    dbInstance = db.getDbInstance()

    result = dbInstance.runGetQuery("SELECT * FROM Client WHERE apikey = ? AND revoked = false", [key])

    return len(result) == 1

def issueApiKey(rootKey: str):
    if rootKey != os.environ.get('API_SECRET_ROOT'):
        return None
    
    apikey = str(uuid.uuid4())
    revoked = False

    dbInstance = db.getDbInstance()

    dbInstance.runUpdateQuery("INSERT INTO Client (apikey, revoked) VALUES (?,?)", [apikey, revoked])

    return apikey


def requireApiKey(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        apikey = request.headers.get('X-API-Key')
        
        if not apikey or not validApiKey(apikey):
            return jsonify({"message": "Forbidden: Invalid API key"}), 403
        
        return f(*args, **kwargs)
    
    return decorated_function