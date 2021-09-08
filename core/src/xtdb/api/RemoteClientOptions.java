package xtdb.api;

import java.util.function.Supplier;

public final class RemoteClientOptions {
    /**
     * A supplier function which provides JWT authorization strings for the remote API client to use
     * to connect to a authenticated HTTP Server.
     */
    public final Supplier<String> jwtSupplier;

    public RemoteClientOptions (Supplier<String> jwtSupplier) {
        this.jwtSupplier = jwtSupplier;
    }
}
