package xtdb.cache

import java.net.URI
import java.nio.file.Path
import java.security.MessageDigest
import java.util.HexFormat
import java.util.UUID

// bump whenever the caches' keying or on-disk layout changes, so entries written under one layout are never read under another
private const val CACHE_LAYOUT = "c1"

internal fun cachePrefix(storageUri: URI?): Path =
    if (storageUri == null) Path.of(CACHE_LAYOUT, "open", UUID.randomUUID().toString())
    else Path.of(CACHE_LAYOUT, sha256Hex(storageUri.toString()).take(32))

private fun sha256Hex(s: String): String =
    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.toByteArray()))
