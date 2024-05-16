from typing import Union, Dict, List


class Query:
    def to_json(self): pass

class Sql(Query):
    def __init__(self, sql: str):
        self.sql = sql

    def to_json(self):
        return {"sql": self.sql}
