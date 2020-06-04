package crux.calcite;

import java.util.UUID;

public class UuidFn {
    public UUID eval (String x) {
        return UUID.fromString(x);
    }
}
