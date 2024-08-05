import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
    kotlin("jvm")
    id("org.jetbrains.dokka")
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB Clojure HTTP Client")
            description.set("XTDB Clojure HTTP Server")
        }
    }
}

dependencies {
    api(project(":xtdb-api"))

    api("pro.juxt.clojars-mirrors.hato", "hato", "0.8.2")
    api("pro.juxt.clojars-mirrors.metosin", "reitit-core", "0.5.15")

    api(kotlin("stdlib-jdk8"))
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(17))

tasks.compileJava {
    sourceCompatibility = "11"
    targetCompatibility = "11"
}

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_11)
    }
}
