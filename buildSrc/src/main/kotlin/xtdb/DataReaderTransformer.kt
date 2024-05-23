package xtdb

import clojure.java.api.Clojure
import clojure.lang.PersistentHashMap
import com.github.jengelman.gradle.plugins.shadow.transformers.Transformer
import com.github.jengelman.gradle.plugins.shadow.transformers.TransformerContext
import org.gradle.api.file.FileTreeElement
import shadow.org.apache.tools.zip.ZipEntry
import shadow.org.apache.tools.zip.ZipOutputStream
import java.io.InputStream

class DataReaderTransformer : Transformer {
    private val readers: MutableMap<Any, Any> = mutableMapOf()

    override fun getName() = "data-readers"

    override fun canTransformResource(element: FileTreeElement) =
        element.relativePath.pathString == "data_readers.clj"

    @Suppress("UNCHECKED_CAST")
    private fun readDataReaders(stream: InputStream): Map<Any, Any> =
        Clojure.read(stream.bufferedReader().readText()) as Map<Any, Any>

    override fun transform(context: TransformerContext) {
        readers += readDataReaders(context.`is`)
    }

    override fun hasTransformedResource() = readers.isNotEmpty()

    override fun modifyOutputStream(os: ZipOutputStream, preserveFileTimestamps: Boolean) {
        os.putNextEntry(ZipEntry("data_readers.clj").apply {
            time = TransformerContext.getEntryTimestamp(preserveFileTimestamps, time)
        })

        os.bufferedWriter().run {
            write(PersistentHashMap.create(readers).toString())
            flush()
        }
    }

}
