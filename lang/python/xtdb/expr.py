import uuid as uuid
from datetime import datetime
from .types import Expr, ToQuery, to_query


class Date(Expr):
    def __init__(self, date: datetime):
        self.date = date

    def to_json(self):
        return {"@type": "xt:timestamptz", "@value": self.date.isoformat()}


class UUID(Expr):
    def __init__(self, uuid_):
        self.uuid = uuid_

    def to_json(self):
        return {"@type": "xt:uuid", "@value": str(self.uuid)}


class Sym(Expr):
    def __init__(self, sym: str):
        self.sym = sym

    def to_json(self):
        return {"xt:lvar": self.sym}


class Param(Expr):
    def __init__(self, param: str):
        self.param = param

    def to_json(self):
        return {"xt:param": self.param}


class Call(Expr):
    def __init__(self, f: str, *args: Expr):
        self.f = f
        self.args = args

    def to_json(self):
        return {"xt:call": self.f, "args": [arg.to_json() for arg in self.args]}


class GetField(Expr):
    def __init__(self, expr: Expr, field: str):
        self.expr = expr
        self.field = field

    def to_json(self):
        return {"xt:get": self.expr.to_json(), "field": self.field}


class Exists(Expr):
    def __init__(self, q: ToQuery, *args: Expr):
        self.q = to_query(q)
        self.args = args

    def to_json(self):
        return {"xt:exists": self.q.to_json(), "args": [arg.to_json() for arg in self.args]}


class Pull(Expr):
    def __init__(self, q: ToQuery, *args: Expr):
        self.q = to_query(q)
        self.args = args

    def to_json(self):
        return {"xt:pull": self.q.to_json(), "args": [arg.to_json() for arg in self.args]}


class PullMany(Expr):
    def __init__(self, q: ToQuery, *args: Expr):
        self.q = to_query(q)
        self.args = args

    def to_json(self):
        return {"xt:pullMany": self.q.to_json(), "args": [arg.to_json() for arg in self.args]}
