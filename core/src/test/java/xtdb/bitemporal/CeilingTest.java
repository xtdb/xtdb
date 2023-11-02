package xtdb.bitemporal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CeilingTest {

    private Ceiling ceiling;

    @BeforeEach
    void setUp() {
        ceiling = new Ceiling();
    }

    private void assertValidTimesAre(long... validTimes) {
        assertArrayEquals(validTimes, ceiling.validTimes().toArray());
    }

    private void assertSysTimeCeilingsAre(long... sysTimeCeilings) {
        assertArrayEquals(sysTimeCeilings, ceiling.sysTimeCeilings().toArray());
    }

    @Test
    void testAppliesLogs() {
        assertValidTimesAre(Long.MIN_VALUE, Long.MAX_VALUE);
        assertSysTimeCeilingsAre(Long.MAX_VALUE);

        ceiling.applyLog(4, 4, Long.MAX_VALUE);
        assertValidTimesAre(Long.MIN_VALUE, 4, Long.MAX_VALUE);
        assertSysTimeCeilingsAre(Long.MAX_VALUE, 4);

        // lower the whole ceiling
        ceiling.applyLog(3, 2, Long.MAX_VALUE);
        assertValidTimesAre(Long.MIN_VALUE, 2, Long.MAX_VALUE);
        assertSysTimeCeilingsAre(Long.MAX_VALUE, 3);

        // lower part of the ceiling
        ceiling.applyLog(2, 1, 4);
        assertValidTimesAre(Long.MIN_VALUE, 1, 4, Long.MAX_VALUE);
        assertSysTimeCeilingsAre(Long.MAX_VALUE, 2, 3);

        // replace a range exactly
        ceiling.applyLog(1, 1, 4);
        assertValidTimesAre(Long.MIN_VALUE, 1, 4, Long.MAX_VALUE);
        assertSysTimeCeilingsAre(Long.MAX_VALUE, 1, 3);

        // replace the whole middle section
        ceiling.applyLog(0, 0, 6);
        assertValidTimesAre(Long.MIN_VALUE, 0, 6, Long.MAX_VALUE);
        assertSysTimeCeilingsAre(Long.MAX_VALUE, 0, 3);
    }
}
