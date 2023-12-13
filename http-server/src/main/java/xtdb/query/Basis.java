package xtdb.query;

import java.time.Instant;

public record Basis(TransactionKey atTx, Instant currentTime){};

