package xtdb.query;

import java.time.Instant;
import xtdb.api.TransactionKey;

public record Basis(TransactionKey atTx, Instant currentTime){};

