package xtdb

import clojure.java.api.Clojure
import clojure.lang.PersistentHashMap
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import com.github.jengelman.gradle.plugins.shadow.transformers.ResourceTransformer
import com.github.jengelman.gradle.plugins.shadow.transformers.TransformerContext
import org.gradle.api.file.FileTreeElement
import org.apache.tools.zip.ZipEntry
import org.apache.tools.zip.ZipOutputStream
import java.io.InputStream

class DataReaderTransformer : ResourceTransformer {
    private val readers: MutableMap<Any, Any> = mutableMapOf()

    override fun getName() = "data-readers"

    override fun canTransformResource(element: FileTreeElement) =
        element.relativePath.pathString == "data_readers.clj"

    @Suppress("UNCHECKED_CAST")
    private fun readDataReaders(stream: InputStream): Map<Any, Any> =
        Clojure.read(stream.bufferedReader().readText()) as Map<Any, Any>

    override fun transform(context: TransformerContext) {
        readers += readDataReaders(context.inputStream)
    }

    override fun hasTransformedResource() = readers.isNotEmpty()

    override fun modifyOutputStream(os: ZipOutputStream, preserveFileTimestamps: Boolean) {
        os.putNextEntry(ZipEntry("data_readers.clj").apply {
            if (!preserveFileTimestamps) time = ShadowJar.CONSTANT_TIME_FOR_ZIP_ENTRIES
        })

        os.bufferedWriter().run {
            write(PersistentHashMap.create(readers).toString())
            flush()
        }
    }

}
