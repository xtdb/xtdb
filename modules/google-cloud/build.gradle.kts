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

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

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

    api("com.google.cloud", "google-cloud-storage", "2.38.0") {
        exclude("com.google.guava","listenablefuture")
    }

    api("com.google.guava","guava","32.1.1-jre")

    api(kotlin("stdlib-jdk8"))
    api(libs.kotlinx.coroutines)
    testImplementation(libs.kotlinx.coroutines.test)
}
