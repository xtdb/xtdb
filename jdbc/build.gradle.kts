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
            name.set("XTDB JDBC")
            description.set("JDBC driver for XTDB")
        }
    }
}

dependencies {
    api(project(":xtdb-api"))

    compileOnlyApi(files("src/main/resources"))
    api(kotlin("stdlib-jdk8"))
    implementation("org.postgresql:postgresql:42.7.5")

    testImplementation(libs.next.jdbc)
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

tasks.compileJava {
    sourceCompatibility = "21"
    targetCompatibility = "21"
}

tasks.compileTestJava {
    sourceCompatibility = "21"
    targetCompatibility = "21"
}

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_21)
    }
}
