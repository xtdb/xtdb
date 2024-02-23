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
    implementation("org.clojure", "clojure", "1.11.1")
    api("org.clojure", "spec.alpha", "0.3.218")

    api("com.widdindustries", "time-literals", "0.1.10")
    api("com.cognitect", "transit-clj", "1.0.333")
    api("com.cognitect", "transit-java", "1.0.371")

    api("org.apache.arrow", "arrow-algorithm", "14.0.0")
    api("org.apache.arrow", "arrow-compression", "14.0.0")
    api("org.apache.arrow", "arrow-vector", "14.0.0")
    api("org.apache.arrow", "arrow-memory-netty", "14.0.0")
    api(kotlin("stdlib-jdk8"))
    api("org.jetbrains.kotlinx", "kotlinx-serialization-json", "1.6.0")

    api("com.github.ben-manes.caffeine", "caffeine", "3.1.8")
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(17))

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
