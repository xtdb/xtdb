package xtdb.authz

import xtdb.query.IQuerySource
import xtdb.api.query.PrepareOpts
import xtdb.api.query.QueryOpts
import xtdb.query.parseStatement
import xtdb.table.TableRef
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.time.Instant

object RoleMembership {

    const val PRIMARY_DB = "xtdb"
    private const val SCAN_SQL = "SELECT \"user\", \"role\" FROM xt.role_membership"

    private val messageDigest = ThreadLocal.withInitial { MessageDigest.getInstance("SHA-256") }

    /**
     * The `_iid` for a membership row, derived from the logical key `(user, role)` — not random —
     * so successive `GRANT`/`REVOKE` on the same pair supersede each other (last-write-wins over
     * system time). Length-prefixing the user keeps the encoding injective.
     */
    @JvmStatic
    fun membershipIid(user: String, role: String): ByteArray {
        val userBytes = user.toByteArray()
        val roleBytes = role.toByteArray()
        val buf = ByteBuffer.allocate(Int.SIZE_BYTES + userBytes.size + roleBytes.size)
            .putInt(userBytes.size).put(userBytes).put(roleBytes)
        return messageDigest.get().digest(buf.array()).copyOfRange(0, 16)
    }

    /**
     * The current within-XT role membership (`user`, `role` pairs) read from the primary's
     * `xt.role_membership` at the node watermark. A fresh nested query — `IQuerySource.openQuery`
     * rebinds the basis from this query alone, so it reflects the current node state, never an outer
     * query's basis (the node-global shape, like `pg_user` / `pg_database`).
     *
     * Skips the scan until the table exists (in a block or the live index): a `pg_roles` read on a
     * node where no membership has ever been written would otherwise raise an unknown-table planner
     * warning, ticking the `query.warning` metric the metrics tests count exactly.
     */
    @JvmStatic
    fun scanMemberships(querySource: IQuerySource, dbCat: IQuerySource.QueryCatalog): List<List<String>> {
        val primary = dbCat.databaseOrNull(PRIMARY_DB) ?: return emptyList()
        val tableRef = TableRef("xt", "role_membership")

        val exists = primary.queryState.tableCatalog.getTypes(tableRef) != null ||
                primary.openSnapshot(null).use { it.tableInfo().containsKey(tableRef) }
        if (!exists) return emptyList()

        val now = Instant.now()
        val prepareOpts = PrepareOpts(currentTime = now, defaultDb = PRIMARY_DB)

        val out = ArrayList<List<String>>()
        querySource.prepareQuery(parseStatement(SCAN_SQL), dbCat, prepareOpts)
            .openQuery(null, QueryOpts(currentTime = now)).use { cursor ->
                cursor.forEachRemaining { rel ->
                    val userRdr = rel.vectorFor("user")
                    val roleRdr = rel.vectorFor("role")
                    repeat(rel.rowCount) { idx ->
                        out.add(listOf(userRdr.getObject(idx) as String, roleRdr.getObject(idx) as String))
                    }
                }
            }
        return out
    }
}
