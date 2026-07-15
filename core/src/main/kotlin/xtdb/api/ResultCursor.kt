package xtdb.api

import xtdb.arrow.VectorType
import java.util.SequencedMap

interface ResultCursor : ICursor {
    val resultTypes: SequencedMap<String, VectorType>
}