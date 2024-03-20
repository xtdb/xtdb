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


ToQuery = Union[Query, tuple[Query, *List[QueryTail]]]
ToBindings = Union[str, Dict[str, 'expr.Expr']]


class Pipeline(Query):
    def __init__(self, q: Query, *qs: QueryTail):
        self.q = q
        self.qs = qs

    def to_json(self):
        return [self.q.to_json(), *[qt.to_json() for qt in self.qs]]


def to_query(q: ToQuery) -> Query:
    if isinstance(q, Query):
        return q

    return Pipeline(q[0], *q[1:])
