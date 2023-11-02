package xtdb.bitemporal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class PolygonTest {

    private static final long MAX_LONG = Long.MAX_VALUE;

    private Polygon polygon;
    private Ceiling ceiling;

    @BeforeEach
    void setUp() {
        polygon = new Polygon();
        ceiling = new Ceiling();
    }

    void applyEvent(long sysFrom, long validFrom, long validTo) {
        polygon.calculateFor(ceiling, validFrom, validTo);
        ceiling.applyLog(sysFrom, validFrom, validTo);
    }

    void assertValidTimesAre(long... validTimes) {
        assertArrayEquals(validTimes, polygon.validTimes().toArray());
    }

    void assertSysTimeCeilingsAre(long... sysTimeCeilings) {
        assertArrayEquals(sysTimeCeilings, polygon.sysTimeCeilings().toArray());
    }

    @Test
    void testCalculateForEmptyCeiling() {
        applyEvent(0, 2, 3);
        assertValidTimesAre(2, 3);
        assertSysTimeCeilingsAre(MAX_LONG);
    }

    @Test
    void startsBeforeNoOverlap() {
        applyEvent(1, 2005, 2009);
        assertValidTimesAre(2005, 2009);
        assertSysTimeCeilingsAre(MAX_LONG);

        applyEvent(0, 2010, 2020);
        assertValidTimesAre(2010, 2020);
        assertSysTimeCeilingsAre(MAX_LONG);
    }

    @Test
    void startsBeforeAndOverlaps() {
        applyEvent(1, 2010, 2020);
        assertValidTimesAre(2010, 2020);
        assertSysTimeCeilingsAre(MAX_LONG);

        applyEvent(0, 2015, 2025);
        assertValidTimesAre(2015, 2020, 2025);
        assertSysTimeCeilingsAre(1, MAX_LONG);
    }

    @Test
    void startsEquallyAndOverlaps() {
        applyEvent(1, 2010, 2020);
        assertValidTimesAre(2010, 2020);
        assertSysTimeCeilingsAre(MAX_LONG);

        applyEvent(0, 2010, 2025);
        assertValidTimesAre(2010, 2020, 2025);
        assertSysTimeCeilingsAre(1, MAX_LONG);
    }

    @Test
    void newerPeriodCompletelyCovered() {
        applyEvent(1, 2015, 2020);
        applyEvent(0, 2010, 2025);

        assertValidTimesAre(2010, 2015, 2020, 2025);
        assertSysTimeCeilingsAre(MAX_LONG, 1, MAX_LONG);
    }

    @Test
    void olderPeriodCompletelyCovered() {
        applyEvent(1, 2010, 2025);
        applyEvent(0, 2010, 2020);

        assertValidTimesAre(2010, 2020);
        assertSysTimeCeilingsAre(1);
    }

    @Test
    void periodEndsEquallyAndOverlaps() {
        applyEvent(1, 2015, 2025);
        applyEvent(0, 2010, 2025);

        assertValidTimesAre(2010, 2015, 2025);
        assertSysTimeCeilingsAre(MAX_LONG, 1);
    }

    @Test
    void periodEndsAfterAndOverlaps() {
        applyEvent(1, 2015, 2025);
        applyEvent(0, 2010, 2020);

        assertValidTimesAre(2010, 2015, 2020);
        assertSysTimeCeilingsAre(MAX_LONG, 1);
    }

    @Test
    void periodStartsBeforeAndTouches() {
        applyEvent(1, 2005, 2010);
        applyEvent(0, 2010, 2020);

        assertValidTimesAre(2010, 2020);
        assertSysTimeCeilingsAre(MAX_LONG);
    }

    @Test
    void periodStartsAfterAndTouches() {
        applyEvent(1, 2010, 2020);
        applyEvent(0, 2005, 2010);

        assertValidTimesAre(2005, 2010);
        assertSysTimeCeilingsAre(MAX_LONG);
    }

    @Test
    void periodStartsAfterAndDoesNotOverlap() {
        applyEvent(1, 2010, 2020);
        applyEvent(0, 2005, 2009);

        assertValidTimesAre(2005, 2009);
        assertSysTimeCeilingsAre(MAX_LONG);
    }
}