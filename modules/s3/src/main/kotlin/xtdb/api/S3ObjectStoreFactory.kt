package xtdb.api

import xtdb.util.requiringResolve
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.ObjectStoreFactory
import xtdb.s3.S3Configurator
import java.nio.file.Path

class DefaultS3Configurator: S3Configurator

class S3ObjectStoreFactory @JvmOverloads constructor(
        val bucket: String,
        val snsTopicArn: String,
        var prefix: Path? = null,
        var s3Configurator: S3Configurator = DefaultS3Configurator()
) : ObjectStoreFactory {
    companion object {
        private val OPEN_OBJECT_STORE = requiringResolve("xtdb.s3", "open-object-store")
    }

    fun prefix(prefix: Path) = apply { this.prefix = prefix }
    fun s3Configurator(s3Configurator: S3Configurator) = apply { this.s3Configurator = s3Configurator }

    override fun openObjectStore() = OPEN_OBJECT_STORE.invoke(this) as ObjectStore
}