package xtdb.postgres

import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.testcontainers.lifecycle.Startables
import org.testcontainers.postgresql.PostgreSQLContainer
import xtdb.api.Xtdb
import xtdb.postgres.proto.PostgresSourceToken
import java.nio.file.Files
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * The external source's minimum Postgres permissions, as a grid: one shared logical-replication
 * container (PUBLIC stripped, a published `widgets` table) against which each case grants a different
 * subset to the connecting role and asserts the outcome — the full set streams, and dropping any one
 * attribute or privilege fails with its own Postgres error.
 *
 * Minimum set: the LOGIN + REPLICATION role attributes, CONNECT on the database, USAGE on the schema,
 * and SELECT on the published tables. CONNECT and USAGE come free from PUBLIC on a stock database, so
 * the shared setup revokes PUBLIC and each role's grants become the whole story.
 *
 * Its own container (not the shared singleton), since the cases churn roles and the setup mutates the
 * container's PUBLIC acl.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PostgresSourcePermissionsTest : PostgresSourceTestBase() {

    private val pg = PostgreSQLContainer("postgres:17-alpine")
        .withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")
        .withCommand("postgres", "-c", "wal_level=logical",
            "-c", "max_replication_slots=50", "-c", "max_wal_senders=50")

    @BeforeAll
    fun setUp() {
        Startables.deepStart(pg).join()
        // shared, immutable fixture: strip PUBLIC so each role's grants are explicit, then a published
        // table for the cases to snapshot. No case mutates these, so the snapshot is identical each run.
        pgExecute(pg, "REVOKE ALL ON DATABASE testdb FROM PUBLIC", "REVOKE ALL ON SCHEMA public FROM PUBLIC")
        pgExecute(pg,
            "CREATE TABLE widgets (_id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO widgets (_id, name) VALUES (1, 'alpha'), (2, 'beta')",
            "CREATE PUBLICATION xtdb_pub FOR TABLE widgets",
        )
    }

    @AfterAll
    fun tearDown() {
        pg.close()
    }

    /** Which of the five the connecting role gets; defaults to the full minimal set. */
    data class Perms(
        val login: Boolean = true, val replication: Boolean = true,
        val connect: Boolean = true, val usage: Boolean = true, val select: Boolean = true,
    )

    sealed interface Expect
    data object Streams : Expect
    data class FailsWith(val errorFragment: String) : Expect

    data class Case(val name: String, val perms: Perms, val expect: Expect) {
        override fun toString() = name
    }

    fun cases() = listOf(
        Case("full set streams",    Perms(),                    Streams),
        Case("without LOGIN",       Perms(login = false),       FailsWith("is not permitted to log in")),
        Case("without REPLICATION", Perms(replication = false), FailsWith("WAL sender")),
        Case("without CONNECT",     Perms(connect = false),     FailsWith("permission denied for database")),
        Case("without USAGE",       Perms(usage = false),       FailsWith("permission denied for schema")),
        Case("without SELECT",      Perms(select = false),      FailsWith("permission denied for table")),
    )

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    fun `permission`(case: Case) = runTest(timeout = 120.seconds) {
        val role = unique("role")
        val attrs = buildList {
            if (case.perms.login) add("LOGIN")
            if (case.perms.replication) add("REPLICATION")
        }.joinToString(" ")
        pgExecute(pg,
            "CREATE ROLE $role WITH $attrs PASSWORD 'pw'",
            *buildList {
                if (case.perms.connect) add("GRANT CONNECT ON DATABASE testdb TO $role")
                if (case.perms.usage) add("GRANT USAGE ON SCHEMA public TO $role")
                if (case.perms.select) add("GRANT SELECT ON widgets TO $role")
            }.toTypedArray(),
        )

        // dirs: log, storage, cdc-log, cdc-storage. Fresh node per case so a failed attach can't bleed
        // into the next; the dedicated container is torn down in @AfterAll, so slots needn't be reclaimed.
        val dirs = List(4) { Files.createTempDirectory("perms") }
        openNode(dirs[0], dirs[1], pg = pg, username = role, password = "pw").use { node ->
            attachCdc(node, database = "cdc", cdcLog = dirs[2], cdcStorage = dirs[3], slot = unique("slot"), pub = "xtdb_pub")

            val err = streamsCleanlyOrError(node)
            when (val expect = case.expect) {
                is Streams -> {
                    assertNull(err, "expected a clean stream, got: $err")
                    assertEquals(setOf("alpha", "beta"),
                        xtQuery(node, database = "cdc", sql = "SELECT name FROM public.widgets").map { it["name"] }.toSet())
                }

                is FailsWith -> {
                    val msg = assertNotNull(err, "expected a failure containing '${expect.errorFragment}', but it streamed")
                    assertTrue(msg.contains(expect.errorFragment), "expected '${expect.errorFragment}', got: $msg")
                }
            }
        }
    }

    /** Polls until the source streams (snapshot complete) or surfaces an ingestion error; returns null
     *  on a clean stream, otherwise the Postgres error message (or a timeout note). */
    private suspend fun streamsCleanlyOrError(node: Xtdb, timeout: Duration = 30.seconds): String? {
        val cat = (node as Xtdb.XtdbInternal).dbCatalog
        val deadline = System.currentTimeMillis() + timeout.inWholeMilliseconds
        while (System.currentTimeMillis() < deadline) {
            cat["cdc"]?.ingestionError?.let { return it.message ?: it.toString() }
            cat["cdc"]?.watchers?.externalSourceToken?.let {
                if (PostgresSourceToken.parseFrom(it).snapshotCompleted) return null
            }
            runInterruptible { Thread.sleep(200) }
        }
        return "timed out waiting for streaming (no ingestionError surfaced)"
    }
}
