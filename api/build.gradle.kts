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
}

tasks.javadoc {
    exclude("xtdb/util/*")
}

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_11)

        java {
            freeCompilerArgs.add("-Xjvm-default=all")
        }
    }
}
