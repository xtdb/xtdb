import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import xtdb.DataReaderTransformer

plugins {
    `java-library`
    alias(libs.plugins.clojurephant)
    alias(libs.plugins.kotlin.jvm)
    signing
    id("com.gradleup.shadow")
}

ext {
    set("labs", true)
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":xtdb-api"))

    api(kotlin("stdlib"))
    implementation("org.apache.kafka:connect-api:4.0.0")

    api(libs.clojure.tools.logging)
    api("cheshire", "cheshire", "5.13.0")
    api(libs.next.jdbc)
    api(libs.pgjdbc)

    testImplementation(testFixtures(project(":")))
    testImplementation(project(":xtdb-core"))
}

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_21)
    }
}

tasks.compileJava {
    sourceCompatibility = "21"
    targetCompatibility = "21"
}

tasks.shadowJar {
    transform(DataReaderTransformer())
    mergeServiceFiles()
    archiveFileName.set("xtdb-kafka-connect.jar")
}
