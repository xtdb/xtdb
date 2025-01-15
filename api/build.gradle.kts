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
            name.set("XTDB API")
            description.set("XTDB API")
        }
    }
}

dependencies {
    compileOnlyApi(files("src/main/resources"))
    api(libs.clojure.spec)

    api(libs.transit.clj)
    api(libs.transit.java)

    api(libs.arrow.algorithm)
    api(libs.arrow.compression)
    api(libs.arrow.vector)
    api(libs.arrow.memory.netty)

    api(kotlin("stdlib-jdk8"))
    api(libs.kotlinx.serialization.json)

    api(libs.caffeine)
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

tasks.javadoc {
    exclude("xtdb/util/*")
}

tasks.compileJava {
    sourceCompatibility = "11"
    targetCompatibility = "11"
}

tasks.compileTestJava {
    sourceCompatibility = "11"
    targetCompatibility = "11"
}

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_11)

        java {
            freeCompilerArgs.add("-Xjvm-default=all")
        }
    }
}

tasks.dokkaHtmlPartial {
    moduleName.set("xtdb-api")
}
