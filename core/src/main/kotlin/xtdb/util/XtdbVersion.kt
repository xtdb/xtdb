package xtdb.util

import java.util.jar.Manifest

object XtdbVersion {
    @JvmStatic
    fun manifestAttribute(key: String): String? =
        ClassLoader.getSystemResource("META-INF/MANIFEST.MF")
            ?.openStream()
            ?.use { Manifest(it).mainAttributes.getValue(key) }

    @JvmStatic
    val version: String by lazy {
        System.getenv("XTDB_VERSION")?.trim()?.takeIf { it.isNotEmpty() }
            ?: manifestAttribute("Implementation-Version")
            ?: "2.x"
    }
}
