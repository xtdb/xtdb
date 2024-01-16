package xtdb.api

import xtdb.util.requiringResolve
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.ObjectStoreFactory
import java.nio.file.Path

class GoogleCloudObjectStoreFactory @JvmOverloads constructor(
        val projectId: String,
        val bucket: String,
        val pubsubTopic: String,
        var prefix: Path? = null,
) : ObjectStoreFactory {
    companion object {
        private val OPEN_OBJECT_STORE = requiringResolve("xtdb.google-cloud", "open-object-store")
    }

    fun prefix(prefix: Path) = apply { this.prefix = prefix }

    override fun openObjectStore() = OPEN_OBJECT_STORE.invoke(this) as ObjectStore
}