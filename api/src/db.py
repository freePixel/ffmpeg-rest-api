import sqlite3
from typing import List, Any
import logging
import threading


class DB:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.lock = threading.Lock()

    def _connect(self):
        return sqlite3.connect(self.db_name)
    
    def runGetQuery(self, query: str, args: List[Any] = []) -> List[tuple]:
        with self.lock:
            conn = self._connect()
            cursor = conn.cursor() 

            try:
                cursor.execute(query, args)
                results = cursor.fetchall()
            except sqlite3.Error as e:
                logging.error(f"An error ocurred: {e}")
                return []
            
            finally:
                cursor.close()
                conn.close()

            return results
    
    def runUpdateQuery(self, query: str, args: List[Any] = []) -> int:
        with self.lock:
            conn = self._connect()
            cursor = conn.cursor()
            try:
                cursor.execute(query, args)
                conn.commit()
                affected_rows = cursor.rowcount
            except sqlite3.Error as e:
                logging.error(f"An error ocurred: {e}, for query '{query}' with args {str(args)}")
                conn.rollback()
                affected_rows = 0

            finally:
                cursor.close()
                conn.close()

            return affected_rows
    
    
    
    def runScript(self, script: str):
        with self.lock:
            conn = self._connect()
            cursor = conn.cursor()

            try:
                cursor.executescript(script)
                conn.commit()
            except sqlite3.Error as e:
                logging.error(f"An error occurred: {e}")
                conn.rollback()
            finally:
                cursor.close()
                conn.close()
        

_instance = None

def runStartupSchema(db: DB):
    script = None
    with open("/app/schema.sql") as f:
        script = f.read()

    logging.info("Updating schema ...")
    try:
        db.runScript(script)
    except Exception as e:
        logging.error("Failed to run update schema, exiting")
        exit(1)

    logging.info("Schema updated sucessfully")

def getDbInstance() -> DB:
    global _instance
    if _instance is None:
        _instance = DB("/sqlite_data/db.sqlite")
        runStartupSchema(_instance)

    return _instance
