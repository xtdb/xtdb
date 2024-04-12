import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    //`maven-publish`
    //signing
    kotlin("jvm")
    kotlin("plugin.serialization")
    //id("org.jetbrains.dokka")
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))
    api(project(":xtdb-http-server"))

    api("ring", "ring-core", "1.10.0")
    api("info.sunng", "ring-jetty9-adapter", "0.22.4")
    api("org.eclipse.jetty", "jetty-alpn-server", "10.0.15")
    api("metosin", "reitit-core", "0.5.15")
    api("pro.juxt.clojars-mirrors.integrant", "integrant", "0.8.0")

    //testImplementation("cheshire", "cheshire", "5.12.0")
}

tasks.javadoc.get().enabled = false
