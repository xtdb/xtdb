import json
from dateutil import parser
from datetime import datetime
from typing import Optional, List

import urllib3
import sqlparse

from .types import Sql, Query
from .xt_json import XtdbJsonEncoder, XtdbJsonDecoder

class TxKey:
    def __init__(self, tx_id: int, system_time: datetime):
        self.tx_id = tx_id
        self.system_time = system_time

    def __str__(self):
        return f"<TxKey '{self.tx_id}' '{self.system_time.isoformat()}'>"

    def __repr__(self):
        return self.__str__()


def _parse_tx_response(tx_response):
    tx_id = tx_response['txId']
    system_time_iso = tx_response['systemTime']
    system_time = parser.parse(system_time_iso)

    return TxKey(tx_id, system_time)


def _latest_tx(latest_submitted_tx: Optional[TxKey], tx_key: TxKey) -> TxKey:
    if latest_submitted_tx is None or tx_key.tx_id > latest_submitted_tx.tx_id:
        return tx_key
    else:
        return latest_submitted_tx


class Xtdb:
    _http = urllib3.PoolManager()

    def __init__(self, url):
        self._latest_submitted_tx = None
        self._url = url
        self._basis = None
        self._import_system_time = None
        self._at_tx = None

    def __str__(self):
        return f"<Xtdb '{self._url}'>"

    def __repr__(self):
        return self.__str__()

    def set_basis(self, timestamp):
        assert datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
        self._basis = timestamp
        self._at_tx = None

    def set_at_tx(self, tx_key: TxKey):
        assert isinstance(tx_key, TxKey)
        basis_time = datetime.strftime(tx_key.system_time, "%Y-%m-%dT%H:%M:%SZ")
        self._at_tx = {"txId": tx_key.tx_id, "systemTime": basis_time}
        self._basis = None

    def set_import_system_time(self, timestamp):
        assert datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
        self._import_system_time = timestamp

    def status(self):
        try:
            resp = self._http.request(
                'GET',
                f"{self._url}/status",
                headers={"accept": "application/json",}
            )
            resp_data = json.loads(resp.data.decode('utf-8'))
            
            if resp.status != 200:
                raise Exception(f"Error getting status: {resp.status} ({resp.reason}): {resp_data}")

            return resp_data
        except Exception as e:
            raise Exception(f"Error in status request: {e}")

    def submit_tx(self, ops: List[Query], opts=None):
        if opts is None:
            opts = {}

        if self._import_system_time:
            opts["systemTime"] = self._import_system_time

        try:
            req_data = json.dumps({"txOps": ops, "opts": opts}, cls=XtdbJsonEncoder)
            resp = self._http.request(
                'POST',
                f"{self._url}/tx",
                body=req_data.encode('utf-8'),
                headers={"accept": "application/json", "content-type": "application/json"}
            )
            resp_data = json.loads(resp.data.decode('utf-8'))

            if resp.status != 200:
                raise Exception(f"Error submitting tx: {resp.status} ({resp.reason}): {resp_data}")

            tx_key = _parse_tx_response(resp_data)
            self._latest_submitted_tx = _latest_tx(self._latest_submitted_tx, tx_key)

            return tx_key
        except Exception as e:
            raise Exception(f"Error in submitTx request: {e}")

    def query(self, query: Query, opts=None):
        if not opts:
            opts = {}

        if self._basis:
            if "basis" not in opts:
                opts["basis"] = {}
            opts["basis"]["currentTime"] = self._basis

        if self._at_tx:
            if "basis" not in opts:
                opts["basis"] = {}
            opts["basis"]["atTx"] = self._at_tx

        try:
            req_data = json.dumps({"query": query, "queryOpts": opts}, cls=XtdbJsonEncoder)
            print(req_data)

            resp = self._http.request(
                'POST',
                f"{self._url}/query",
                body=req_data.encode('utf-8'),
                headers={"accept": "application/json", "content-type": "application/json"}
            )

            resp_data = json.loads(resp.data.decode('utf-8'), cls=XtdbJsonDecoder)

            if resp.status != 200:
                raise Exception(f"Error submitting query: {resp.status} ({resp.reason}): {resp_data}")

            return resp_data
        except Exception as e:
            raise Exception(f"Error in query request: {e}")

class Error(Exception):
    pass

class Warning(Exception):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class InternalError(DatabaseError):
    pass

class OperationalError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class DataError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    pass

class SQLParsingError(DatabaseError):
    def __init__(self, message):
        super().__init__(message)

class DBAPI(object):
    paramstyle = "qmark"
    apilevel = "2.0"
    threadsafty = 0 # not thought about, so lowest level

    Warning = Warning
    Error = Error

    def connect(self, url):
      return Connection(url)

def check_closed(f):
    """Decorator that checks if connection/cursor is closed."""

    def g(self, *args, **kwargs):
        if self.closed:
            raise Error(
                '{klass} already closed'.format(klass=self.__class__.__name__))
        return f(self, *args, **kwargs)
    return g

def check_result(f):
    """Decorator that checks if the cursor has results from `execute`."""

    def g(self, *args, **kwargs):
        if self._results is None:
            raise Error('Called before `execute`')
        return f(self, *args, **kwargs)
    return g

_tx_ops = ["INSERT INTO", "UPDATE", "DELETE FROM", "ERASE FROM"]
_session_vars = ["import_system_time", "basis"]

def remove_trailing_semicolon(s: str):
    if s[-1] == ';':
        return s[:-1]

def _is_set(operation):
    if operation.startswith("SET"):
        for var in _session_vars:
            if var in operation:
                timestamp = operation.split(" = ")[-1].replace("'", "")
                if timestamp == "None":
                    timestamp = None
                return [var, timestamp]
        return (False, None)
    return (False, None)

def _is_tx(operation):
    for op in _tx_ops:
        if op in operation:
            return True
    return False

class Connection(object):
    
    def __init__(self, url):
        self.client = Xtdb(url)

        self.closed = False
        self.cursors = []

    @check_closed
    def close(self):
        """Close the connection now."""
        self.closed = True
        for cursor in self.cursors:
            try:
                cursor.close()
            except Error:
                pass  # already closed

    @check_closed
    def commit(self):
        """
        Commit any pending transaction to the database.

        Not supported.
        """
        pass

    @check_closed
    def cursor(self):
        """Return a new Cursor Object using the connection."""
        cursor = Cursor(self.client)
        self.cursors.append(cursor)

        return cursor

    @check_closed
    def execute(self, operation, parameters=None):
        cursor = self.cursor()
        return cursor.execute(operation, parameters)
    
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.commit()  # no-op
        self.close()

class Cursor(object):

    """Connection cursor."""

    def __init__(self, client):
        self.client = client

        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # this is updated only after a query
        self.description = None

        # this is set to a list of rows after a successful query
        self._results = None

    @property
    @check_result
    @check_closed
    def rowcount(self):
        return len(self._results)

    @check_closed
    def close(self):
        """Close the cursor."""
        self.closed = True


    def _execute_operations(self, operations):
        if operations == []:
            self._results = []
        
        # If only transactions found, execute as transactions
        elif all([_is_tx(op) for op in operations]):
            self.client.submit_tx([Sql(op) for op in operations])
            self._results = []

        # If only queries found, execute as queries
        elif all([not _is_tx(op) for op in operations]):
            for op in operations:
                self._results = self.client.query(Sql(op))

        # Cannot mix them
        else:
            raise SQLParsingError(f"Cannot mix queries and transactions: {operations}")


    def _execute_set_statements(self, operations):
        if operations == []:
            return []
        
        first_op, *rest_ops = operations
        set_variable, variable_value = _is_set(first_op)

        match set_variable:
            case "import_system_time":
                # Set import time
                if variable_value:
                    self.client.set_import_system_time(variable_value)
                else:
                    self.client._import_system_time = None
                
                return self._execute_set_statements(rest_ops)

            case "basis":
                if variable_value:
                    self.client.set_basis(variable_value)
                else:
                    self.client._basis = None
                    
                return self._execute_set_statements(rest_ops)
            
            case False:
                if not all([not _is_set(op)[0] for op in rest_ops]):
                    raise SQLParsingError(f"Can only add SET statements to beginning of session: {rest_ops}")
                return operations

            case _:
                raise SQLParsingError(f"Unhandled SET variable: {set_variable}") 
        
    @check_closed
    def execute(self, operation, parameters=None):
        self.description = None
        # Ignoring parameters for now (don't think jupysql uses them)

        operations = [remove_trailing_semicolon(s) for s in sqlparse.split(operation)]

        if operations == [None]:
            raise SQLParsingError(f"Parsing Error on {operation}, no SQL statements found by driver API internals")

        stripped_operations = self._execute_set_statements(operations)

        self._execute_operations(stripped_operations)
        
        cols = {}
        for row in self._results:
            for name, value in row.items():
                if name not in cols:
                    cols[name] = value

        self.description = [
            (
                name,                       # name
                type(value),                # type_code
                None,                       # [display_size]
                None,                       # [internal_size]
                None,                       # [precision]
                None,                       # [scale]
                True,                       # [null_ok]
            )
            for name, value in cols.items()
        ]

        self._results = [[*result.values()] for result in self._results]

        return self

    @check_closed
    def executemany(self, operation, seq_of_parameters=None):
        raise NotSupportedError(
            '`executemany` is not supported, use `execute` instead')

    @check_result
    @check_closed
    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single sequence,
        or `None` when no more data is available.
        """
        try:
            return self._results.pop(0)
        except IndexError:
            return None

    @check_result
    @check_closed
    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
        """
        size = size or self.arraysize
        out = self._results[:size]
        self._results = self._results[size:]
        return out

    @check_result
    @check_closed
    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        out = self._results[:]
        self._results = []
        return out

    @check_closed
    def setinputsizes(self, sizes):
        # not supported
        pass

    @check_closed
    def setoutputsizes(self, sizes):
        # not supported
        pass

    @check_closed
    def __iter__(self):
        return iter(self._results)
