from uuid import UUID
from datetime import datetime
from typing import List, Optional, Union, Dict, Any, TypedDict
from .expr import Expr, Date as dateExpr
from .binding import Binding, to_bindings
from .types import UnifyClause, Query, ToBindings, XtqlQuery

EntityId = Union[int, str, UUID]


class TxOp:
    def to_json(self):
        pass


class PutDocs(TxOp):
    def __init__(self, into: str, *put_docs):
        self.into = into
        self.put_docs = put_docs
        self.valid_from: Optional[datetime] = None
        self.valid_to: Optional[datetime] = None

    def starting_from(self, valid_from: datetime) -> 'PutDocs':
        self.valid_from = valid_from
        return self

    def until(self, valid_to: datetime) -> 'PutDocs':
        self.valid_to = valid_to
        return self

    def during(self, valid_from: Optional[datetime] = None, valid_to: Optional[datetime] = None) -> 'PutDocs':
        self.valid_from = valid_from
        self.valid_to = valid_to
        return self

    def to_json(self):
        return {
            "putDocs": self.put_docs,
            "into": self.into,
            "validFrom": self.valid_from.isoformat() if self.valid_from else None,
            "validTo": self.valid_to.isoformat() if self.valid_to else None
        }


class DeleteDocs(TxOp):
    def __init__(self, delete_docs: List[EntityId], from_: str):
        self.delete_docs = delete_docs
        self.from_ = from_
        self.valid_from: Optional[datetime] = None
        self.valid_to: Optional[datetime] = None

    def starting_from(self, valid_from: datetime) -> 'DeleteDocs':
        self.valid_from = valid_from
        return self

    def until(self, valid_to: datetime) -> 'DeleteDocs':
        self.valid_to = valid_to
        return self

    def during(self, valid_from: Optional[datetime] = None, valid_to: Optional[datetime] = None) -> 'DeleteDocs':
        self.valid_from = valid_from
        self.valid_to = valid_to
        return self

    def to_json(self):
        return {
            "deleteDocs": self.delete_docs,
            "from": self.from_,
            "validFrom": self.valid_from.isoformat() if self.valid_from else None,
            "validTo": self.valid_to.isoformat() if self.valid_to else None
        }


class EraseDocs(TxOp):
    def __init__(self, erase_docs: List[EntityId], from_: str):
        self.erase_docs = erase_docs
        self.from_ = from_

    def to_json(self):
        return {
            "eraseDocs": self.erase_docs,
            "from": self.from_
        }


class Sql(TxOp):
    def __init__(self, sql: str):
        self.sql = sql
        self.arg_rows: Optional[List[List[Any]]] = None

    def args(self, *arg_rows: List[Any]) -> 'Sql':
        self.arg_rows = list(arg_rows)
        return self

    def to_json(self):
        return {
            "sql": self.sql,
            "argRows": self.arg_rows
        }


ArgRow = Dict[str, Any]


class Insert(TxOp):
    def __init__(self, insert_into: str, query: XtqlQuery):
        self.insert_into = insert_into
        self.query = query
        self.arg_rows: Optional[List[ArgRow]] = None

    def args(self, *args: ArgRow) -> 'Insert':
        self.arg_rows = list(args)
        return self

    def to_json(self):
        return {
            "insertInto": self.insert_into,
            "query": self.query.to_json(),
            "args": self.arg_rows
        }


TemporalExtents = Union[str, Dict[str, Optional[Expr]]]


def _as_expr(date: Optional[Union[datetime, Expr]]) -> Optional[Expr]:
    if date is None:
        return None
    if isinstance(date, datetime):
        return dateExpr(date)
    return date


class Update(TxOp):
    def __init__(self, update: str):
        self.update = update
        self.for_valid_time: TemporalExtents = {}
        self.bind: List[Binding] = []
        self.set: List[Binding] = []
        self.unify: List[UnifyClause] = []
        self.arg_rows: Optional[List[ArgRow]] = None

    def starting_from(self, from_: Union[datetime, Expr]) -> 'Update':
        return self.during(from_, None)

    def until(self, to: Union[datetime, Expr]) -> 'Update':
        return self.during(None, to)

    def during(self, from_: Optional[Union[datetime, Expr]] = None,
               to: Optional[Union[datetime, Expr]] = None) -> 'Update':
        self.for_valid_time = {'from': _as_expr(from_), 'to': _as_expr(to)}
        return self

    def for_all_time(self) -> 'Update':
        self.for_valid_time = "allTime"
        return self

    def binding(self, *bindings: ToBindings) -> 'Update':
        self.bind.extend(out_binding
                         for in_binding in bindings
                         for out_binding in to_bindings(in_binding))
        return self

    def setting(self, *bindings: ToBindings) -> 'Update':
        self.set.extend(out_binding
                        for in_binding in bindings
                        for out_binding in to_bindings(in_binding))
        return self

    def unifying(self, *clauses: UnifyClause) -> 'Update':
        self.unify.extend(clauses)
        return self

    def args(self, *arg_rows: ArgRow) -> 'Update':
        self.arg_rows = list(arg_rows)

        return self

    def to_json(self):
        return {
            "update": self.update,
            "forValidTime": self.for_valid_time,
            "bind": [bind.to_json() for bind in self.bind],
            "set": [s.to_json() for s in self.set],
            "unify": [clause.to_json() for clause in self.unify],
            "args": self.arg_rows
        }


class Delete(TxOp):
    def __init__(self, delete_from: str):
        self.delete_from = delete_from
        self.for_valid_time: TemporalExtents = {}
        self.bind: List[Binding] = []
        self.unify: List[UnifyClause] = []
        self.arg_rows: Optional[List[ArgRow]] = None

    def starting_from(self, from_: Union[datetime, Expr]) -> 'Delete':
        return self.during(from_, None)

    def until(self, to: Union[datetime, Expr]) -> 'Delete':
        return self.during(None, to)

    def during(self, from_: Optional[Union[datetime, Expr]] = None,
               to: Optional[Union[datetime, Expr]] = None) -> 'Delete':
        self.for_valid_time = {'from': _as_expr(from_), 'to': _as_expr(to)}
        return self

    def for_all_time(self) -> 'Delete':
        self.for_valid_time = "allTime"
        return self

    def binding(self, *bindings: ToBindings) -> 'Delete':
        self.bind.extend(out_binding
                         for in_binding in bindings
                         for out_binding in to_bindings(in_binding))
        return self

    def unifying(self, *clauses: UnifyClause) -> 'Delete':
        self.unify.extend(clauses)
        return self

    def args(self, *arg_rows: ArgRow) -> 'Delete':
        self.arg_rows = list(arg_rows)
        return self

    def to_json(self):
        return {
            "deleteFrom": self.delete_from,
            "forValidTime": self.for_valid_time,
            "bind": [bind.to_json() for bind in self.bind],
            "unify": [clause.to_json() for clause in self.unify],
            "args": self.arg_rows
        }


class Erase(TxOp):
    def __init__(self, from_: str):
        self.from_ = from_
        self.bind: List[Binding] = []
        self.unify: List[UnifyClause] = []
        self.arg_rows: Optional[List[ArgRow]] = None

    def binding(self, *bindings: ToBindings) -> 'Erase':
        self.bind.extend(out_binding
                         for in_binding in bindings
                         for out_binding in to_bindings(in_binding))
        return self

    def unifying(self, *clauses: UnifyClause) -> 'Erase':
        self.unify.extend(clauses)
        return self

    def args(self, *arg_rows: ArgRow) -> 'Erase':
        self.arg_rows = list(arg_rows)
        return self

    def to_json(self):
        return {
            "erase": self.from_,
            "bind": [bind.to_json() for bind in self.bind],
            "unify": [clause.to_json() for clause in self.unify],
            "args": self.arg_rows
        }


class AssertExists(TxOp):
    def __init__(self, assert_exists: XtqlQuery):
        self.assert_exists = assert_exists
        self.arg_rows: Optional[List[ArgRow]] = None

    def args(self, *arg_rows: ArgRow) -> 'AssertExists':
        self.arg_rows = list(arg_rows)
        return self

    def to_json(self):
        return {
            "assertExists": self.assert_exists.to_json(),
            "args": self.arg_rows
        }


class AssertNotExists(TxOp):
    def __init__(self, assert_not_exists: XtqlQuery):
        self.assert_not_exists = assert_not_exists
        self.arg_rows: Optional[List[ArgRow]] = None

    def args(self, *arg_rows: ArgRow) -> 'AssertNotExists':
        self.arg_rows = list(arg_rows)
        return self

    def to_json(self):
        return {
            "assertNotExists": self.assert_not_exists.to_json(),
            "args": self.arg_rows
        }
