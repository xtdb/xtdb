import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    `java-library`
    alias(libs.plugins.clojurephant)
    `maven-publish`
    signing
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.dokka)
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
    api(libs.transit.clj)

    api(libs.arrow.algorithm)
    api(libs.arrow.compression)
    api(libs.arrow.vector)
    api(libs.arrow.memory.netty)
    api(libs.netty.common)
    api(libs.arrow.adbc)
    api(libs.arrow.adbc.driver.manager)
    api(libs.arrow.flight.sql)

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
    api(libs.micrometer.tracing)
    api(libs.micrometer.tracing.bridge.otel)
    api(libs.opentelemetry.exporter.otlp)
    api(libs.opentelemetry.sdk)

    api(kotlin("stdlib"))
    api(libs.kotlinx.coroutines)
    testImplementation(libs.kotlinx.coroutines.test)
    api(libs.kaml)

    api(libs.protobuf.kotlin)

    api(libs.next.jdbc)

    antlr(libs.antlr)
    implementation(libs.antlr.runtime)

    api(libs.hato)
    testImplementation(libs.pgjdbc)
    testImplementation(libs.mockk)
    testImplementation(libs.kotest.props)
    testImplementation(libs.clojure.test.check)
    testImplementation(testFixtures(project(":")))
    testImplementation(project(":xtdb-main"))
    testImplementation(project(":modules:xtdb-kafka"))
    testImplementation(project(":modules:xtdb-aws"))
    testImplementation(project(":modules:xtdb-google-cloud"))
    testImplementation(project(":modules:xtdb-azure"))
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
