package xtdb.util

import org.apache.arrow.vector.ipc.SeekableReadChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ

internal fun Path.newSeekableByteChannel() = SeekableReadChannel(Files.newByteChannel(this, READ))

val String.asPath: Path
    get() = Path.of(this)