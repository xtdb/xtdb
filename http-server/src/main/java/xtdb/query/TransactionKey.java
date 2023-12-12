package xtdb.query;

import java.time.Instant;

// TODO remove xtdb.api.TransactionKey and replace with this
public record TransactionKey(Long txId, Instant systemTime) {}
