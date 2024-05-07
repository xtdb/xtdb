import json
from dateutil import parser
from datetime import datetime
from typing import Optional

import urllib3

from .types import ToQuery, Sql
from .xt_json import XtdbJsonEncoder, XtdbJsonDecoder
from . import query as q


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

    def __str__(self):
        return f"<Xtdb '{self._url}'>"

    def __repr__(self):
        return self.__str__()

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

    def submit_tx(self, ops, opts=None):
        if opts is None:
            opts = {}

        try:
            req_data = json.dumps({"txOps": ops, **opts}, cls=XtdbJsonEncoder)
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

    def query(self, query: ToQuery, opts=None):
        if opts is None:
            opts = {}

        try:
            req_data = json.dumps({"query": query, "queryOpts": opts}, cls=XtdbJsonEncoder)

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
def is_tx(operation):
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

    @check_closed
    def execute(self, operation, parameters=None):
        self.description = None
        # Ignoring parameters for now (don't think jupysql uses them)

        # ✨ Quick and dirty ✨
        if is_tx(operation):
            self.client.submit_tx([Sql(operation)])
            self._results = []
        else:
            self._results = self.client.query(Sql(operation))

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
