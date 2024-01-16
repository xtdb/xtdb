package xtdb.api

import xtdb.util.requiringResolve
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.ObjectStoreFactory
import java.nio.file.Path

class AzureObjectStoreFactory @JvmOverloads constructor(
        val storageAccount: String,
        val container: String,
        val servicebusNamespace: String,
        val servicebusTopicName: String,
        var prefix: Path? = null,
) : ObjectStoreFactory {
    companion object {
        private val OPEN_OBJECT_STORE = requiringResolve("xtdb.azure", "open-object-store")
    }

    fun prefix(prefix: Path) = apply { this.prefix = prefix }

    override fun openObjectStore() = OPEN_OBJECT_STORE.invoke(this) as ObjectStore
}