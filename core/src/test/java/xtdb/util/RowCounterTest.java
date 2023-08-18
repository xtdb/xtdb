package xtdb.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RowCounterTest {

    @Test
    void testRowCounter() {
        var rc = new RowCounter(24);

        assertEquals(24, rc.getChunkIdx());

        assertEquals(0, rc.getChunkRowCount());

        rc.addRows(15);
        assertEquals(15, rc.getChunkRowCount());

        rc.addRows(15);
        assertEquals(30, rc.getChunkRowCount());
    }
}
