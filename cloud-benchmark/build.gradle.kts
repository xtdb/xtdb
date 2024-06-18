import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    kotlin("jvm")
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))
    api(project(":xtdb-http-server"))
    implementation(project(":modules:bench"))
    
    api("ch.qos.logback", "logback-classic", "1.4.5")
    api("clj-http", "clj-http","3.12.3")
    api("cheshire","cheshire","5.10.1")
}

sourceSets {
    main {
        resources {
            srcDir("../src/test/resources")
        }
    }
}

