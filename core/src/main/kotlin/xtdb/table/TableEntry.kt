package xtdb.table

import xtdb.api.TableRef
import xtdb.block.proto.TableEntry as TableEntryProto

typealias Oid = Long

/** A table's entry in the block's registry: its identity, and the directory its files live in. */
data class TableEntry(val oid: Oid, val table: TableRef, val slug: TableSlug) {

    fun toProto(): TableEntryProto =
        TableEntryProto.newBuilder()
            .setOid(oid.toInt())
            .setSchemaName(table.schemaName)
            .setTableName(table.tableName)
            .setSlug(slug.slug)
            .build()

    companion object {
        /**
         * The lowest oid we hand out. Postgres reserves everything below this for its own catalog objects,
         * and tooling filters on the boundary to tell a user's tables from the system's — so a table
         * numbered from 1 reads as a system catalog to the clients we publish the oid to.
         */
        const val FIRST_OID: Oid = 16384L

        /** Mints an entry for a table that has none. Minting one for a table that does orphans its files. */
        @JvmStatic
        fun mint(oid: Oid, table: TableRef) = TableEntry(oid, table, TableSlug.of(table))

        @JvmStatic
        fun fromProto(proto: TableEntryProto) =
            TableEntry(
                // the proto field is uint32, which the generated accessor hands back as a signed Int
                proto.oid.toUInt().toLong(),
                TableRef(proto.schemaName, proto.tableName),
                TableSlug(proto.slug)
            )
    }
}
