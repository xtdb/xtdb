from datetime import datetime
from enum import Enum
from typing import List, Dict, Optional, Any

from . import expr
from . import temporal_filter as tf
from .binding import Binding, to_bindings
from .temporal_filter import TemporalFilter
from .types import UnifyClause, QueryTail, Query, ToQuery, Expr, ToBindings, to_query

Args = List[Binding]


class From(UnifyClause, Query):
    class TemporalFilterConfigurator:
        def __init__(self, outer: 'From', k: str):
            self.outer = outer
            self.k = k

        def starting(self, from_: datetime) -> 'From':
            self.outer.__setattr__(self.k, tf.From(from_))
            return self.outer

        def until(self, to: datetime) -> 'From':
            self.outer.__setattr__(self.k, tf.To(to))
            return self.outer

        def during(self, from_: datetime, to: datetime) -> 'From':
            self.outer.__setattr__(self.k, tf.In(from_, to))
            return self.outer

        def all_time(self) -> 'From':
            self.outer.__setattr__(self.k, tf.AllTime.AllTime)
            return self.outer

    def __init__(self,
                 table: str,
                 for_valid_time: Optional[TemporalFilter] = None,
                 for_system_time: Optional[TemporalFilter] = None,
                 projectAllCols=False):
        self.table = table
        self._for_valid_time = for_valid_time
        self._for_system_time = for_system_time
        self.projectAllCols = projectAllCols
        self.bind: List[Binding] = []

    def binding(self, *bindings: ToBindings) -> 'From':
        self.bind.extend(out_binding
                         for in_binding in bindings
                         for out_binding in to_bindings(in_binding))
        return self

    def for_valid_time(self) -> TemporalFilterConfigurator:
        return self.TemporalFilterConfigurator(self, 'for_valid_time')

    def for_system_time(self) -> TemporalFilterConfigurator:
        return self.TemporalFilterConfigurator(self, 'for_system_time')

    def to_json(self):
        return {
            'from': self.table,
            'forValidTime': self._for_valid_time.to_json() if self._for_valid_time else None,
            'forSystemTime': self._for_system_time.to_json() if self._for_system_time else None,
            'projectAllCols': self.projectAllCols,
            'bind': [bind.to_json() for bind in self.bind]
        }


class Where(UnifyClause, QueryTail):
    def __init__(self, *preds: Expr):
        self.preds = list(preds)

    def to_json(self):
        return {"where": [pred.to_json() for pred in self.preds]}


class Join(UnifyClause):
    def __init__(self, query: ToQuery, args=None):
        self.query: Query = to_query(query)
        self.args = args if args else []
        self.bindings: List[Binding] = []

    def binding(self, *bindings: ToBindings) -> 'Join':
        self.bindings.extend(out_binding
                             for in_binding in bindings
                             for out_binding in to_bindings(in_binding))
        return self

    def to_json(self):
        return {
            'join': self.query.to_json(),
            'args': [arg.to_json() for arg in self.args],
            'bindings': [binding.to_json() for binding in self.bindings]
        }


class LeftJoin(UnifyClause):
    bindings: list[Any] = []

    def __init__(self, query: ToQuery, args: List[Dict[str, 'expr.Expr']]):
        self.query: Query = to_query(query)
        self.args = args

    def binding(self, *bindings: ToBindings) -> 'LeftJoin':
        self.bindings.extend(out_binding
                             for in_binding in bindings
                             for out_binding in to_bindings(in_binding))
        return self

    def to_json(self):
        return {
            'leftJoin': self.query.to_json(),
            'args': [{k: v.to_json() for k, v in arg.items()} for arg in self.args],
            'bindings': [binding.to_json() for binding in self.bindings]
        }


class Aggregate(QueryTail):
    def __init__(self, *bindings: ToBindings):
        self.bindings = [out_binding
                         for in_binding in bindings
                         for out_binding in to_bindings(in_binding)]

    def to_json(self):
        return {"aggregate": [binding.to_json() for binding in self.bindings]}


class With(QueryTail, UnifyClause):
    def __init__(self, *bindings: ToBindings):
        self.bindings = [out_binding
                         for in_binding in bindings
                         for out_binding in to_bindings(in_binding)]

    def to_json(self):
        return {"with": [binding.to_json() for binding in self.bindings]}


class Return(QueryTail):
    def __init__(self, *bindings: ToBindings):
        self.bindings = [out_binding
                         for in_binding in bindings
                         for out_binding in to_bindings(in_binding)]

    def to_json(self):
        return {"return": [binding.to_json() for binding in self.bindings]}


class Without(QueryTail):
    def __init__(self, *cols: str):
        self.cols = list(cols)

    def to_json(self):
        return {"without": self.cols}


class Unnest(QueryTail, UnifyClause):
    def __init__(self, binding: str, expr_: 'expr.Expr'):
        self.binding = binding
        self.expr = expr_

    def to_json(self):
        return {"unnest": {self.binding: self.expr.to_json()}}


class OrderDir(Enum):
    ASC = "asc"
    DESC = "desc"


class OrderNulls(Enum):
    FIRST = "first"
    LAST = "last"


class OrderSpec:
    def __init__(self, val: 'expr.Expr', direction: OrderDir = OrderDir.ASC, nulls: OrderNulls = OrderNulls.FIRST):
        self.val = val
        self.direction = direction
        self.nulls = nulls

    def asc(self):
        self.direction = OrderDir.ASC
        return self

    def desc(self):
        self.direction = OrderDir.DESC
        return self

    def nulls_first(self):
        self.nulls = OrderNulls.FIRST
        return self

    def nulls_last(self):
        self.nulls = OrderNulls.LAST
        return self

    def to_json(self):
        return {"val": self.val.to_json(), "dir": self.direction.value, "nulls": self.nulls.value}


class OrderBy(QueryTail):
    def __init__(self, *specs: OrderSpec):
        self.specs = specs

    def to_json(self):
        return {"orderBy": [spec.to_json() for spec in self.specs]}


class Limit(QueryTail):
    def __init__(self, limit: int):
        self.limit = limit

    def to_json(self):
        return {"limit": self.limit}


class Offset(QueryTail):
    def __init__(self, offset: int):
        self.offset = offset

    def to_json(self):
        return {"offset": self.offset}


class Unify(Query):
    def __init__(self, *clauses: UnifyClause):
        self.clauses = list(clauses)

    def to_json(self):
        return {"unify": [clause.to_json() for clause in self.clauses]}
