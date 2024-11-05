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
    api("metosin", "jsonista", "0.3.3")
    api(libs.reitit.core)
    api(libs.reitit.interceptors)
    api(libs.reitit.ring)
    api(libs.reitit.http)
    api(libs.reitit.sieppari)
    api(libs.reitit.swagger)
    api(libs.reitit.spec)

    api("com.cognitect", "transit-clj", "1.0.329")

    api(kotlin("stdlib-jdk8"))
    implementation(kotlin("reflect"))
    implementation("org.jetbrains.kotlinx", "kotlinx-serialization-json", "1.6.0")

    testImplementation(project(":"))
    testImplementation(project(":xtdb-http-client-jvm"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.9.2")
    testImplementation("pro.juxt.clojars-mirrors.hato", "hato", "0.8.2")
    // hato uses cheshire for application/json encoding
    testImplementation("cheshire", "cheshire", "5.12.0")
}

tasks.javadoc.get().enabled = false
