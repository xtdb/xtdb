import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    `java-library`
    alias(libs.plugins.clojurephant)
    `maven-publish`
    signing
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.dokka)
}

ext {
    set("labs", true)
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB ADBC Driver")
            description.set("In-process ADBC driver for XTDB")
        }
    }
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    api("org.apache.arrow", "arrow-vector", "15.0.2")
    api(libs.arrow.adbc)

    api(kotlin("stdlib"))

    testImplementation(testFixtures(project(":")))
}
