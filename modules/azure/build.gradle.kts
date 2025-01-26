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

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB Azure")
            description.set("XTDB Azure")
        }
    }
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    api("com.azure", "azure-storage-blob", "12.27.0")
    api("com.azure", "azure-identity", "1.13.1")
    api("com.azure", "azure-core-management", "1.15.1")

    // metrics
    api(libs.micrometer.registry.azuremonitor)

    api(kotlin("stdlib-jdk8"))
    api(libs.kotlinx.coroutines)
    testImplementation(libs.kotlinx.coroutines.test)
}
