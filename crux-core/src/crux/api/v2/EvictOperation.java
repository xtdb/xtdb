package crux.api.v2;

import clojure.lang.Keyword;
import clojure.lang.PersistentVector;

import java.util.Date;

// TODO: Only allow withEndValidTime, keepLatest and keepEarliest if previous is set

public class EvictOperation extends Operation {
    private final Date validTime;
    private final Date endValidTime;
    private final Boolean keepLatest;
    private final Boolean keepEarliest;
    private final CruxId evictId;

    private EvictOperation(CruxId evictId, Date validTime, Date endValidTime, Boolean keepLatest, Boolean keepEarliest) {
        this.evictId = evictId;
        this.validTime = validTime;
        this.endValidTime = endValidTime;
        this.keepLatest = keepLatest;
        this.keepEarliest = keepEarliest;
    }

    public static EvictOperation evictOp(CruxId evictId) {
        return new EvictOperation(evictId, null, null, null, null);
    }

    public EvictOperation withValidTime(Date validTime) {
        return new EvictOperation(evictId, validTime, endValidTime, keepLatest, keepEarliest);
    }

    public EvictOperation withEndValidTime(Date endValidTime) {
        return new EvictOperation(evictId, validTime, endValidTime, keepLatest, keepEarliest);
    }

    public EvictOperation keepLatest(Boolean keepLatest) {
        return new EvictOperation(evictId, validTime, endValidTime, keepLatest, keepEarliest);
    }

    public EvictOperation keepEarliest(Boolean keepEarliest) {
        return new EvictOperation(evictId, validTime, endValidTime, keepLatest, keepEarliest);
    }

    @Override
    protected PersistentVector toEdn() {
        PersistentVector outputVector = PersistentVector.create(Keyword.intern("crux.tx/evict"), evictId.toEdn());
        if(validTime != null)
            outputVector = outputVector.cons(validTime);
        if(endValidTime != null)
            outputVector = outputVector.cons(endValidTime);
        if(keepLatest != null)
            outputVector = outputVector.cons(keepLatest);
        if(validTime != null)
            outputVector = outputVector.cons(validTime);
        return outputVector;
    }
}
