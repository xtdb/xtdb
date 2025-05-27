import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
    kotlin("jvm")
    kotlin("plugin.serialization")
    id("org.jetbrains.dokka")
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB HTTP Server")
            description.set("XTDB HTTP Server")
        }
    }
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    api(libs.ring.core)
    api(libs.ring.jetty9.adapter)
    api(libs.jetty.alpn.server)

    api(libs.muuntaja)
    api(libs.jsonista)
    api(libs.reitit.core)
    api(libs.reitit.interceptors)
    api(libs.reitit.ring)
    api(libs.reitit.http)
    api(libs.reitit.sieppari)
    api(libs.reitit.swagger)
    api(libs.reitit.spec)

    api(libs.transit.clj)

    api(kotlin("stdlib-jdk8"))
    implementation(kotlin("reflect"))
    implementation(libs.kotlinx.serialization.json)

    testImplementation(project(":"))
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.hato)
    // hato uses cheshire for application/json encoding
    testImplementation("cheshire", "cheshire", "5.12.0")
}

tasks.javadoc.get().enabled = false
