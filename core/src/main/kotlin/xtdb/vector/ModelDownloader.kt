package xtdb.vector

import ai.djl.repository.zoo.Criteria
import java.io.File
import kotlin.system.exitProcess

/**
 * Utility to download embedding models at build time
 */
object ModelDownloader {
    
    @JvmStatic
    fun main(args: Array<String>) {
        if (args.size != 2) {
            println("Usage: ModelDownloader <model-name> <output-dir>")
            exitProcess(1)
        }
        
        val modelName = args[0]
        val outputDir = File(args[1])
        
        try {
            println("Downloading model: $modelName to ${outputDir.absolutePath}")
            
            val criteria = Criteria.builder()
                .setTypes(String::class.java, FloatArray::class.java)
                .optModelUrls("djl://ai.djl.huggingface.pytorch/$modelName")
                .build()
            
            // This will download the model to DJL's cache
            val model = criteria.loadModel()
            println("Model downloaded successfully to DJL cache: ${model.modelPath}")
            
            // Copy to our resources directory for packaging
            val modelPath = model.modelPath
            val targetDir = File(outputDir, modelName.substringAfterLast('/'))
            targetDir.mkdirs()
            
            // Copy model files
            modelPath.toFile().copyRecursively(targetDir, overwrite = true)
            println("Model copied to resources: ${targetDir.absolutePath}")
            
            model.close()
            
        } catch (e: Exception) {
            println("Failed to download model: ${e.message}")
            e.printStackTrace()
            exitProcess(1)
        }
    }
}