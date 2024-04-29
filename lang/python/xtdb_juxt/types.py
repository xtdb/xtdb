from typing import Union, Dict, List


class Expr:
    def to_json(self):
        pass


class UnifyClause:
    def to_json(self): pass


class QueryTail:
    def to_json(self): pass


class Query:
    def to_json(self): pass

    
class XtqlQuery(Query):
    def to_json(self): pass

class Sql(Query):
    def __init__(self, sql: str):
        self.sql = sql

    def to_json(self):
        return {"sql": self.sql}

ToQuery = Union[Query, tuple]
ToBindings = Union[str, Dict[str, 'Expr']]


class Pipeline(XtqlQuery):
    def __init__(self, q: XtqlQuery, *qs: QueryTail):
        self.q = q
        self.qs = qs

    def to_json(self):
        return [self.q.to_json(), *[qt.to_json() for qt in self.qs]]


def to_query(q: ToQuery) -> XtqlQuery:
    if isinstance(q, Query):
        return q

    return Pipeline(q[0], *q[1:])
