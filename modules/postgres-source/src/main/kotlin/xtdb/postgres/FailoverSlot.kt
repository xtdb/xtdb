package xtdb.postgres

import xtdb.api.RemoteAlias
import xtdb.api.Xtdb
import xtdb.api.error.Fault
import xtdb.api.error.Incorrect
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

/** `pg_replication_slots.failover`, and `ALTER_REPLICATION_SLOT` to set it, both arrived in PG17. */
private const val MIN_SERVER_VERSION_NUM = 170_000

/**
 * Enables `failover` on a logical replication slot, so that PG17+ slot synchronisation copies it to
 * a standby and it survives promotion. Without it a promoted standby holds no logical slot at all,
 * and the source can only recover by re-snapshotting — see #5828.
 *
 * pgjdbc's replication API has no `FAILOVER` option, so every slot XTDB creates starts without it
 * and an operator has to flip the flag between stopping the source and restarting it. This is that
 * step, with the ways it goes wrong *quietly* turned into refusals — the ones Postgres already
 * rejects loudly are left to Postgres.
 */
object FailoverSlot {

    /** A logical replication slot, as `pg_replication_slots` reports it. */
    data class Slot(
        val name: String,
        val database: String,
        val active: Boolean,
        val activePid: Int?,
        val failover: Boolean,
    )

    sealed interface Outcome {
        /** Which server was altered, as `host:port/database` — deliberately pre-rendered, so an
         * outcome that gets logged can't carry the remote's password with it. */
        val server: String

        val slot: Slot

        /** `failover` was already set, so nothing was altered. */
        data class AlreadyEnabled(override val server: String, override val slot: Slot) : Outcome

        data class Enabled(override val server: String, val before: Slot, val after: Slot) : Outcome {
            override val slot get() = after
        }
    }

    /** Resolves [alias] against the `remotes:` block of [configFile] — the operator has already
     * named the upstream there, so the command takes that rather than a second set of credentials. */
    @JvmStatic
    fun enable(configFile: Path, alias: RemoteAlias, slotName: String): Outcome {
        val remotes = Xtdb.readConfig(configFile).remotes

        val factory = remotes[alias]
            ?: throw Incorrect(
                "no remote '$alias' in $configFile — defined: ${remotes.keys.sorted().joinToString().ifEmpty { "none" }}",
                errorCode = "xtdb.postgres/missing-remote",
                data = mapOf("alias" to alias, "config-file" to configFile.toString()),
            )

        val pgFactory = factory as? PostgresRemote.Factory
            ?: throw Incorrect(
                "remote '$alias' is a ${factory.javaClass.name}, expected a '!Postgres' remote",
                errorCode = "xtdb.postgres/wrong-remote-type",
                data = mapOf("alias" to alias, "actual-type" to factory.javaClass.name),
            )

        return pgFactory.open().use { enable(it, slotName) }
    }

    @JvmStatic
    fun report(outcome: Outcome): String {
        val headline = when (outcome) {
            is Outcome.AlreadyEnabled -> "Replication slot '${outcome.slot.name}' already had failover enabled — nothing altered."
            is Outcome.Enabled -> "Enabled failover on replication slot '${outcome.slot.name}'."
        }

        return "$headline\n  ${outcome.server}  failover=${outcome.slot.failover}  active=${outcome.slot.active}"
    }

    @JvmStatic
    fun enable(remote: PostgresRemote, slotName: String): Outcome =
        remote.openConnection().use { conn ->
            conn.requireFailoverSlots(remote)

            val before = conn.readSlot(remote, slotName)
                .also { it.checkBelongsTo(remote); it.checkIdle() }

            if (before.failover) return@use Outcome.AlreadyEnabled(remote.describe(), before)

            remote.openReplicationConnection().use { replConn ->
                replConn.createStatement().use { it.execute("ALTER_REPLICATION_SLOT $slotName (FAILOVER true)") }
            }

            val after = conn.readSlot(remote, slotName)

            if (!after.failover)
                throw Fault(
                    "ALTER_REPLICATION_SLOT reported success, but '$slotName' still isn't a failover slot",
                    errorCode = "xtdb.postgres/slot-failover-not-applied",
                    data = mapOf("slot-name" to slotName, "remote" to remote.describe()),
                )

            Outcome.Enabled(remote.describe(), before, after)
        }

    /** Postgres scopes neither `ALTER_REPLICATION_SLOT` nor its errors by database, so a slot name
     * that collides with another database's would be flipped without complaint. */
    private fun Slot.checkBelongsTo(remote: PostgresRemote) {
        if (database != remote.database)
            throw Incorrect(
                "replication slot '$name' belongs to database '$database', not '${remote.database}'" +
                        " — check the slot name against the remote",
                errorCode = "xtdb.postgres/slot-wrong-database",
                data = mapOf("slot-name" to name, "slot-database" to database, "remote-database" to remote.database),
            )
    }

    /** `ALTER_REPLICATION_SLOT` parks indefinitely on an active slot rather than erroring — and then
     * applies whenever the consumer eventually goes away, which is its own surprise. */
    private fun Slot.checkIdle() {
        if (active)
            throw Incorrect(
                "replication slot '$name' is still active (pid $activePid)" +
                        " — stop every node streaming it, then retry",
                errorCode = "xtdb.postgres/slot-active",
                data = mapOf("slot-name" to name, "active-pid" to activePid),
            )
    }

    private fun PostgresRemote.describe() = "$hostname:$port/$database"

    private fun PostgresRemote.openConnection(): Connection =
        DriverManager.getConnection("jdbc:postgresql://$hostname:$port/$database", username, password)

    private fun PostgresRemote.openReplicationConnection(): Connection =
        openReplicationConnection(hostname, port, database, username, password)

    /** Checked before anything reads `failover`, which pre-PG17 servers have no column for, and
     * before the ALTER, whose absence they report as a bare `syntax error`. */
    private fun Connection.requireFailoverSlots(remote: PostgresRemote) {
        val versionNum = createStatement().use { s ->
            s.executeQuery("SELECT current_setting('server_version_num')::int").use { rs ->
                if (!rs.next()) throw Fault(
                    "no server_version_num from ${remote.describe()}",
                    errorCode = "xtdb.postgres/no-server-version",
                )
                rs.getInt(1)
            }
        }

        if (versionNum < MIN_SERVER_VERSION_NUM)
            throw Incorrect(
                "failover slots need PostgreSQL 17 or later; ${remote.describe()} is ${versionNum / 10_000}",
                errorCode = "xtdb.postgres/failover-slots-unsupported",
                data = mapOf("remote" to remote.describe(), "server-version-num" to versionNum),
            )
    }

    private fun Connection.readSlot(remote: PostgresRemote, slotName: String): Slot =
        prepareStatement(
            """
            SELECT database, active, active_pid, failover
            FROM pg_replication_slots
            WHERE slot_name = ? AND slot_type = 'logical'
            """.trimIndent()
        ).use { ps ->
            ps.setString(1, slotName)
            ps.executeQuery().use { rs ->
                if (!rs.next()) refuseNoLogicalSlot(remote, slotName)

                Slot(
                    name = slotName,
                    database = rs.getString("database"),
                    active = rs.getBoolean("active"),
                    activePid = rs.getInt("active_pid").takeUnless { rs.wasNull() },
                    failover = rs.getBoolean("failover"),
                )
            }
        }

    /** Only reached when the happy-path read found nothing, so it can afford a second query to say
     * which of the two it was. */
    private fun Connection.refuseNoLogicalSlot(remote: PostgresRemote, slotName: String): Nothing {
        val slotType = prepareStatement("SELECT slot_type FROM pg_replication_slots WHERE slot_name = ?").use { ps ->
            ps.setString(1, slotName)
            ps.executeQuery().use { rs -> if (rs.next()) rs.getString(1) else null }
        }

        if (slotType == null) {
            throw Incorrect(
                "no replication slot '$slotName' on ${remote.describe()}",
                errorCode = "xtdb.postgres/slot-not-found",
                data = mapOf("slot-name" to slotName),
            )
        } else {
            throw Incorrect(
                "replication slot '$slotName' is a $slotType slot — failover applies to logical slots only",
                errorCode = "xtdb.postgres/slot-not-logical",
                data = mapOf("slot-name" to slotName, "slot-type" to slotType),
            )
        }
    }
}
