package crux.api;

import java.util.Date;

public interface IBitemporalInstant {
    Date validTime();
    Date transactionTime();
}
