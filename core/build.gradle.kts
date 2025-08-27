import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
    kotlin("jvm")
    kotlin("plugin.serialization")
    id("org.jetbrains.dokka")
    antlr

    alias(libs.plugins.protobuf)
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB Core")
            description.set("XTDB Core")
        }
    }
}

dependencies {
    api(project(":xtdb-api"))
    compileOnlyApi(files("src/main/resources"))

    api(libs.clojure.tools.logging)
    api(libs.slf4j.api)

    implementation(libs.clojure)
    api(libs.clojure.spec)
    api(libs.clojure.`data`.json)
    api(libs.clojure.`data`.csv)
    api(libs.clojure.tools.cli)
    api(libs.transit.clj)

    api(libs.arrow.algorithm)
    api(libs.arrow.compression)
    api(libs.arrow.vector)
    api(libs.arrow.memory.netty)
    api(libs.netty.common)

    api(libs.roaring.bitmap)

    api(libs.integrant)
    api(libs.clj.yaml)

    api(libs.commons.codec)
    api(libs.hppc)

    api(libs.caffeine)
    api(libs.buddy.hashers)

    // healthz server
    api(libs.ring.core)
    api(libs.ring.jetty9.adapter)
    api(libs.jetty.alpn.server)

    api(libs.muuntaja)
    api(libs.reitit.core)
    api(libs.reitit.interceptors)
    api(libs.reitit.ring)
    api(libs.reitit.http)
    api(libs.reitit.sieppari)

    // monitoring
    api(libs.micrometer.core)
    api(libs.micrometer.registry.prometheus)

    api(kotlin("stdlib-jdk8"))
    api(libs.kotlinx.coroutines)
    testImplementation(libs.kotlinx.coroutines.test)
    api(libs.kaml)

    api(libs.protobuf.kotlin)

    api(libs.next.jdbc)

    // DJL for embeddings
    api(libs.djl.api)
    api(libs.djl.huggingface)
    api(libs.djl.pytorch.engine)
    runtimeOnly(libs.djl.pytorch.native) // Native library for PyTorch

    antlr(libs.antlr)
    implementation(libs.antlr.runtime)

    testImplementation(libs.pgjdbc)
    testImplementation(libs.mockk)
    testImplementation(libs.clojure.test.check)
    testImplementation(project(":"))
    testImplementation(project(":xtdb-http-server"))
    testImplementation(project(":modules:xtdb-kafka"))
    testImplementation(project(":modules:xtdb-aws"))
    testImplementation(project(":modules:xtdb-google-cloud"))
    testImplementation(project(":modules:xtdb-azure"))
    testImplementation(project(":modules:xtdb-flight-sql"))
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

tasks.javadoc.get().enabled = false

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_21)

        java {
            freeCompilerArgs.add("-Xjvm-default=all")
        }
    }
}

tasks.compileKotlin {
    dependsOn("generateGrammarSource")
}

tasks.compileTestKotlin {
    dependsOn("generateTestGrammarSource")
}

tasks.generateGrammarSource {
    arguments = listOf(
        "-visitor", "-no-listener",
        "-package", "xtdb.antlr",
        "-Xexact-output-dir",
    )
    outputDirectory = file("${layout.buildDirectory.get().asFile}/generated-src/antlr/main/xtdb/antlr")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${libs.versions.protobuf.asProvider().get()}"
    }

    generateProtoTasks {
        all().forEach {
            it.builtins {
                create("kotlin")
            }
        }
    }
}

// Download embedding model at build time (optional)
val downloadEmbeddingModel by tasks.registering {
    description = "Downloads the sentence transformer model for embeddings"
    group = "build setup"
    
    // Only run if property is set
    onlyIf { project.hasProperty("downloadModels") || System.getenv("DOWNLOAD_MODELS") == "true" }
    
    val modelDir = file("${layout.buildDirectory.get().asFile}/resources/main/models/sentence-transformers")
    val modelMarker = file("$modelDir/all-MiniLM-L6-v2/.downloaded")
    
    inputs.property("model-version", "all-MiniLM-L6-v2")
    outputs.file(modelMarker)
    
    doLast {
        println("Downloading embedding model...")
        modelDir.mkdirs()
        
        // Use DJL to download the model
        javaexec {
            mainClass.set("xtdb.vector.ModelDownloader")
            classpath = sourceSets["main"].runtimeClasspath
            args = listOf(
                "sentence-transformers/all-MiniLM-L6-v2",
                modelDir.absolutePath
            )
            // Increase memory for model download
            jvmArgs = listOf("-Xmx2g")
        }
        
        // Create marker file
        modelMarker.writeText("Downloaded at ${System.currentTimeMillis()}")
        println("Model downloaded successfully")
    }
}

// Optionally make compilation depend on model download
if (project.hasProperty("downloadModels") || System.getenv("DOWNLOAD_MODELS") == "true") {
    tasks.compileKotlin {
        dependsOn(downloadEmbeddingModel)
    }
}

tasks.dokkaHtmlPartial {
    dokkaSourceSets["main"].run {
        perPackageOption {
            matchingRegex.set(".*")
            suppress.set(true)
        }

        perPackageOption {
            matchingRegex.set("xtdb\\.api.*")
            suppress.set(false)
        }
    }
}
