from datetime import datetime
import json
import uuid

from .types import Query, QueryTail
from .tx import TxOp


class XTRuntimeError(RuntimeError):
    def __init__(self, message):
        super().__init__(message)

class XtdbJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (Query, QueryTail, TxOp)):
            return obj.to_json()
        elif isinstance(obj, uuid.UUID):
            return {'@type': 'xt:uuid', '@value': str(obj)}
        return super().default(obj)

class XtdbJsonDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        if '@type' in obj:
            if obj['@type'] == 'xt:uuid':
                return uuid.UUID(obj['@value'])
            elif obj['@type'] == 'xt:error':
                raise XTRuntimeError(obj['@value'])
            elif obj['@type'] == 'xt:timestamptz':
                timestamp = datetime.strptime(obj['@value'], "%Y-%m-%dT%I:%M:%S.%fZ[%Z]")
                return timestamp
            else:
                raise ValueError(f"Unknown type: {obj['@type']}")
        return obj
