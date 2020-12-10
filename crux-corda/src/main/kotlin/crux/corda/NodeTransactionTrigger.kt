package crux.corda

import org.h2.api.Trigger
import java.sql.Connection

@Suppress("unused")
class NodeTransactionTrigger : Trigger {
    override fun init(conn: Connection, schemaName: String, triggerName: String, tableName: String, before: Boolean, type: Int) {}
    override fun close() {}
    override fun remove() {}

    override fun fire(conn: Connection, oldRow: Array<out Any>?, newRow: Array<out Any>?) {
        if (newRow != null
            && newRow[3] == "V"
            && (oldRow == null || oldRow[3] == "U")) {

            conn.prepareStatement("INSERT INTO crux_txs (corda_tx_id) VALUES (?)").use { stmt ->
                stmt.setString(1, newRow[0] as String)
                stmt.execute()
            }
        }
    }
}