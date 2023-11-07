package xtdb.bitemporal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class PolygonTest {

    private static final long MAX_LONG = Long.MAX_VALUE;

    private Polygon polygon;
    private Polygon inPolygon;
    private Ceiling ceiling;

    @BeforeEach
    void setUp() {
        polygon = new Polygon();

        inPolygon = new Polygon();
        inPolygon.sysTimeCeilings().add(MAX_LONG);

        ceiling = new Ceiling();
    }

    void setInValidTimes(long... validTimes) {
        inPolygon.validTimes().clear();
        inPolygon.validTimes().add(validTimes);
    }

    void setInSysTimeCeilings(long... sysTimeCeilings) {
        inPolygon.sysTimeCeilings().clear();
        inPolygon.sysTimeCeilings().add(sysTimeCeilings);
    }

    void applyPolygon(long sysFrom) {
        polygon.calculateFor(ceiling, inPolygon);
        ceiling.applyLog(sysFrom, inPolygon.getValidFrom(0), inPolygon.getValidTo(inPolygon.getValidTimeRangeCount() - 1));
    }

    void applyEvent(long sysFrom, long validFrom, long validTo) {
        setInValidTimes(validFrom, validTo);
        applyPolygon(sysFrom);
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

    @Test
    void testCalculatingFromMultiRangePolygon() {
        applyEvent(5, 2012, 2015);
        setInValidTimes(2005, 2009, 2010, Long.MAX_VALUE);
        setInSysTimeCeilings(4, 3, Long.MAX_VALUE);

        applyPolygon(1);
        assertValidTimesAre(2005, 2009, 2010, 2012, 2015, Long.MAX_VALUE);
        assertSysTimeCeilingsAre(4, 3, Long.MAX_VALUE, 5, Long.MAX_VALUE);
    }

    @Test
    void testMultiRangePolygonMeetsNewEvent() {
        applyEvent(5, 2010, 2015);
        setInValidTimes(2005, 2009, 2010, Long.MAX_VALUE);
        setInSysTimeCeilings(4, 3, Long.MAX_VALUE);

        applyPolygon(1);
        assertValidTimesAre(2005, 2009, 2010, 2015, Long.MAX_VALUE);
        assertSysTimeCeilingsAre(4, 3, 5, Long.MAX_VALUE);
    }

    @Test
    void testLaterEventDoesntChangeSupersededEvent() {
        applyEvent(5, 2010, 2015);
        setInValidTimes(2005, 2009, Long.MAX_VALUE);
        setInSysTimeCeilings(Long.MAX_VALUE, 3);

        applyPolygon(1);
        assertValidTimesAre(2005, 2009, Long.MAX_VALUE);
        assertSysTimeCeilingsAre(Long.MAX_VALUE, 3);
    }
}
