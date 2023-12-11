package xtdb.query;

import clojure.lang.Keyword;

import java.time.Duration;
import java.time.ZoneId;
import java.util.List;

public record QueryMap(Query query, List<ArgSpec> args, Basis basis, Object afterTx, Duration txTimeout,
                       ZoneId defaultTz, Boolean explain, Keyword keyFn){};

