package xtdb.query;

import clojure.lang.Keyword;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Map;
import xtdb.api.TransactionKey;

public record QueryMap(Query query, Map<Keyword, Object> args, Basis basis, TransactionKey afterTx, Duration txTimeout,
                       ZoneId defaultTz, Boolean explain, Keyword keyFn){};

