package xtdb;

import java.time.Instant;

/**
 * @see java.time.InstantSource - polyfill until we have Java 17 as a minimum reqmt.
 */
public interface InstantSource {
    Instant instant();

    static InstantSource SYSTEM = new InstantSource() {
        @Override
        public Instant instant() {
            return Instant.now();
        }
    };

    static InstantSource mock(Iterable<Instant> instants) {
        var it = instants.iterator();

        return new InstantSource() {
            @Override
            public Instant instant() {
                assert it.hasNext() : "out of instants";
                return it.next();
            }
        };
    }
}
