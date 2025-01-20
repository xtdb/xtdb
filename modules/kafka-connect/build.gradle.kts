import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import xtdb.DataReaderTransformer

plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    kotlin("jvm")
    signing
    id("com.gradleup.shadow")
}

ext {
    set("labs", true)
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-http-client-jvm"))

    api(kotlin("stdlib-jdk8"))
    implementation("org.apache.kafka:connect-api:3.8.0")

    api("org.clojure", "tools.logging", "1.2.4")
    api("cheshire", "cheshire", "5.13.0")
    api("org.slf4j", "slf4j-api", "1.7.36")
    api("com.github.seancorfield", "next.jdbc", "1.3.939")
    api("org.postgresql", "postgresql", "42.7.3")

    testImplementation(project(":"))
    testImplementation(project(":xtdb-core"))
}

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

tasks.compileJava {
    sourceCompatibility = "17"
    targetCompatibility = "17"
}

tasks.shadowJar {
    transform(DataReaderTransformer())
    mergeServiceFiles()
    archiveFileName.set("xtdb-kafka-connect.jar")
}
