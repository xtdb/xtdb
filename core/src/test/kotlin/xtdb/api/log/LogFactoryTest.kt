package xtdb.api.log

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.storage.Storage
import xtdb.database.Database
import xtdb.util.asPath

class LogFactoryTest {

    private fun roundTrip(factory: Log.Factory): Log.Factory {
        val dbConfig = Database.Config(factory, Storage.local("storage".asPath))
        return Database.Config.fromProto(dbConfig.serializedConfig).log
    }

    @Test
    fun `round-trips in-memory defaults`() {
        val restored = roundTrip(InMemoryLog.Factory()) as InMemoryLog.Factory

        assertEquals(0, restored.epoch)
        assertEquals(0, restored.termEpoch)
    }

    @Test
    fun `round-trips in-memory epoch and termEpoch`() {
        val restored = roundTrip(InMemoryLog.Factory().epoch(42).termEpoch(3)) as InMemoryLog.Factory

        assertEquals(42, restored.epoch)
        assertEquals(3, restored.termEpoch)
    }

    @Test
    fun `round-trips local defaults`() {
        val restored = roundTrip(LocalLog.Factory("log".asPath)) as LocalLog.Factory

        assertEquals("log".asPath, restored.path)
        assertEquals(0, restored.epoch)
        assertEquals(0, restored.termEpoch)
    }

    @Test
    fun `round-trips local epoch and termEpoch`() {
        val original = LocalLog.Factory("log".asPath).epoch(42).termEpoch(3)
        val restored = roundTrip(original) as LocalLog.Factory

        assertEquals("log".asPath, restored.path)
        assertEquals(42, restored.epoch)
        assertEquals(3, restored.termEpoch)
    }
}
