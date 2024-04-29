import json
from dateutil import parser
from datetime import datetime
from typing import Optional

import urllib3

from .types import ToQuery
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
