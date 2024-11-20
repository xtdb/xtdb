import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
    kotlin("jvm")
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB JDBC")
            description.set("JDBC driver for XTDB")
        }
    }
}

dependencies {
    compileOnlyApi(files("src/main/resources"))
    api(kotlin("stdlib-jdk8"))
    implementation("org.postgresql:postgresql:42.2.23")
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

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
    }
}
