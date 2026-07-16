package xtdb.postgres

import io.kotest.property.Arb
import io.kotest.property.arbitrary.*
import io.kotest.property.checkAll
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import xtdb.XtdbInternal
import xtdb.api.Xtdb
import java.nio.file.Files
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Drives random programs of mutations + lifecycle events (insert/update/delete, batches, block
 * flushes, restarts) against a real Postgres container, keeping a reference model and asserting
 * XTDB's CDC pipeline converges to it.
 */
@Tag("property")
class PostgresSourceSimulationTest : PostgresSourceTestBase() {

    // --- program model ---

    private sealed interface Op {
        data class Insert(val id: Int, val name: String) : Op
        data class Update(val id: Int, val name: String) : Op
        data class Delete(val id: Int) : Op
    }

    private sealed interface Cmd {
        data class Mutate(val op: Op) : Cmd
        data class Batch(val ops: List<Op>) : Cmd
        data object FlushBlock : Cmd
        data object Restart : Cmd
        data class RestartWith(val downOps: List<Op>) : Cmd
    }

    // --- reference model + oracle ---

    // id -> name; an UPDATE of a missing row is a no-op in PG, so the model mirrors that
    private fun applyOp(model: Map<Int, String>, op: Op): Map<Int, String> = when (op) {
        is Op.Insert -> model + (op.id to op.name)
        is Op.Update -> if (op.id in model) model + (op.id to op.name) else model
        is Op.Delete -> model - op.id
    }

    private fun opSql(table: String, op: Op): String = when (op) {
        // ON CONFLICT so a generated INSERT of an existing id behaves like the model's overwrite
        is Op.Insert -> "INSERT INTO $table (_id, name) VALUES (${op.id}, '${op.name}') " +
            "ON CONFLICT (_id) DO UPDATE SET name = EXCLUDED.name"
        is Op.Update -> "UPDATE $table SET name = '${op.name}' WHERE _id = ${op.id}"
        is Op.Delete -> "DELETE FROM $table WHERE _id = ${op.id}"
    }

    private fun cdcIngestionError(node: Xtdb) =
        (node as XtdbInternal).dbCatalog["cdc"]?.ingestionError

    private fun qRows(node: Xtdb, table: String): List<Pair<Int, String?>> =
        xtQuery(node, database = "cdc", sql = "SELECT _id, name FROM public.$table ORDER BY _id")
            .map { (it["_id"] as Number).toInt() to (it["name"] as String?) }

    /** The oracle: waits for the cdc pipeline to drain, then asserts the cdc table matches the
     *  reference model. Bails fast if ingestion has stopped — a poisoned db can never catch up — and
     *  on timeout asserts so the failure surfaces the expected-vs-actual diff rather than a bare wait. */
    private suspend fun assertFinalState(node: Xtdb, table: String, model: Map<Int, String>, timeout: Duration = 60.seconds) {
        val expected = model.toSortedMap().map { (id, name) -> id to name }
        val deadline = System.currentTimeMillis() + timeout.inWholeMilliseconds
        while (true) {
            cdcIngestionError(node)?.let { fail("cdc ingestion stopped for $table: ${it.message}") }
            val actual = runCatching { qRows(node, table) }.getOrNull()
            when {
                actual == expected -> return
                System.currentTimeMillis() > deadline ->
                    return assertEquals(expected, actual, "cdc table never converged to the model for $table")
                else -> runInterruptible { Thread.sleep(200) }
            }
        }
    }

    // --- interpreter ---

    /** Creates the table + publication, attaches cdc, drives `program` against Postgres while keeping
     *  the reference model in step, and finally asserts convergence. The node lives in a `var` because
     *  restarts close and reopen it; the initial snapshot is flushed so the snapshotCompleted token is
     *  durable and restarts resume streaming rather than re-running the (uninterruptible) snapshot. */
    private suspend fun runProgram(program: List<Cmd>) {
        val table = unique("pg"); val pub = unique("pub"); val slot = unique("slot")
        // log, storage, cdc-log, cdc-storage
        val dirs = List(4) { Files.createTempDirectory("pg-sim") }
        pgExecute(
            "CREATE TABLE $table (_id INT PRIMARY KEY, name TEXT)",
            "CREATE PUBLICATION $pub FOR TABLE $table",
        )
        var node = openNode(dirs[0], dirs[1])
        var model = emptyMap<Int, String>()
        try {
            attachCdc(node, database = "cdc", cdcLog = dirs[2], cdcStorage = dirs[3], slot = slot, pub = pub)
            awaitStreaming(node)
            flushBlock(node)

            for (cmd in program) {
                when (cmd) {
                    is Cmd.Mutate -> {
                        pgExecute(opSql(table, cmd.op))
                        model = applyOp(model, cmd.op)
                    }
                    is Cmd.Batch -> {
                        // one PG transaction => one commit
                        pgExecute(*(listOf("BEGIN") + cmd.ops.map { opSql(table, it) } + "COMMIT").toTypedArray())
                        model = cmd.ops.fold(model, ::applyOp)
                    }
                    Cmd.FlushBlock -> flushBlock(node)
                    Cmd.Restart -> { node.close(); node = openNode(dirs[0], dirs[1]); awaitStreaming(node) }
                    is Cmd.RestartWith -> {
                        // mutate while CDC is down, so streaming has to catch up on resume
                        node.close()
                        if (cmd.downOps.isNotEmpty()) {
                            pgExecute(*cmd.downOps.map { opSql(table, it) }.toTypedArray())
                            model = cmd.downOps.fold(model, ::applyOp)
                        }
                        node = openNode(dirs[0], dirs[1])
                        // wait for the cdc db to re-attach + resume before touching it again
                        awaitStreaming(node)
                    }
                }
                // bail the moment the db is poisoned rather than running on
                cdcIngestionError(node)?.let { fail("cdc poisoned after $cmd: ${it.message}") }
            }
            assertFinalState(node, table, model)
        } finally {
            node.close()
            runCatching { dropSlot(slot) }
            dirs.forEach { it.toFile().deleteRecursively() }
        }
    }

    // --- generators ---

    private val opGen: Arb<Op> = arbitrary {
        val id = Arb.int(1..8).bind()
        val name = Arb.string(0..8, Codepoint.alphanumeric()).bind()
        when (Arb.int(0..2).bind()) {
            0 -> Op.Insert(id, name)
            1 -> Op.Update(id, name)
            else -> Op.Delete(id)
        }
    }

    // weighted to favour mutations, with the occasional batch / flush / restart; convergence is
    // checked once at the end rather than at mid-program checkpoints
    private val cmdGen: Arb<Cmd> = arbitrary {
        when (Arb.int(0 until 12).bind()) {
            in 0..6 -> Cmd.Mutate(opGen.bind())
            in 7..8 -> Cmd.Batch(Arb.list(opGen, 1..5).bind())
            9 -> Cmd.FlushBlock
            10 -> Cmd.Restart
            else -> Cmd.RestartWith(Arb.list(opGen, 0..3).bind())
        }
    }

    private val programGen: Arb<List<Cmd>> = Arb.list(cmdGen, 1..20)

    // --- tests ---

    @Test
    fun `simulation smoke`() = runTest(timeout = 30.minutes) {
        runProgram(
            listOf(
                Cmd.Mutate(Op.Insert(1, "Alice")),
                Cmd.Batch((3..21).map { Op.Insert(it, "row$it") }),
                Cmd.FlushBlock,
                Cmd.Restart,
                Cmd.RestartWith(listOf(Op.Insert(100, "Charlie"))),
                Cmd.Mutate(Op.Update(1, "AliceUpdated")),
                Cmd.Mutate(Op.Delete(2)),
            )
        )
    }

    @Test
    fun `random programs converge to the reference model`() = runTest(timeout = 30.minutes) {
        checkAll(ITERATIONS, programGen) { program -> runProgram(program) }
    }

    companion object {
        private const val ITERATIONS = 50
    }
}
