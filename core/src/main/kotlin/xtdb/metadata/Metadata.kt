package xtdb.metadata

import org.roaringbitmap.buffer.ImmutableRoaringBitmap
import xtdb.util.TemporalBounds
import xtdb.vector.IVectorReader

interface ITableMetadata {
   fun metadataReader() : IVectorReader

   /**
    * @return the set of column names in this metadata file for this table (i.e. not necessarily all of them).
    */
   fun columnNames() : Set<String>

   fun rowIndex(columnName: String, pageIdx: Int) : Long
   fun iidBloomBitmap(pageIdx: Int) : ImmutableRoaringBitmap
   fun temporalBounds(pageIdx: Int) : TemporalBounds
}

data class PageIndexKey(val columnName: String, val pageIdx: Int)
