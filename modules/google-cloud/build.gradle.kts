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

ext {
    set("labs", true)
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(17))

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB Google Cloud")
            description.set("XTDB Google Cloud")
        }
    }
}

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    api("com.google.cloud", "google-cloud-storage", "2.23.0") {
        exclude("com.google.guava","listenablefuture")
    }
    api("com.google.cloud", "google-cloud-pubsub", "1.124.0") {
        exclude("com.google.guava","listenablefuture")
    }
    api("com.google.guava","guava","32.1.1-jre")

    api(kotlin("stdlib-jdk8"))
}
