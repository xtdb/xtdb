package xtdb.util

import java.nio.file.Path

fun isMetaFile(filePath: Path): Boolean {
    val regex = Regex(""".*meta/.*\.arrow$""")
    return regex.containsMatchIn(filePath.toString())
}

fun isDataFile(filePath: Path): Boolean {
    val regex = Regex(""".*data/.*\.arrow$""")
    return regex.containsMatchIn(filePath.toString())
}

