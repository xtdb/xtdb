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
            name.set("XTDB S3")
            description.set("XTDB S3")
        }
    }
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(17))

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    api("software.amazon.awssdk", "s3", "2.16.76")
    api("software.amazon.awssdk", "sqs", "2.16.76")
    api("software.amazon.awssdk", "sns", "2.16.76")
    api(kotlin("stdlib-jdk8"))
}
