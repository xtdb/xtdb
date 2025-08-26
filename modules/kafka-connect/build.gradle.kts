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

    api(kotlin("stdlib-jdk8"))
    implementation("org.apache.kafka:connect-api:3.9.1")

    api(libs.clojure.tools.logging)
    api("cheshire", "cheshire", "5.13.0")
    api(libs.next.jdbc)
    api(libs.pgjdbc)

    testImplementation(project(":"))
    testImplementation(project(":xtdb-core"))
    testImplementation(libs.hato)

    // Doc examples:
    // TODO: change to testImplementation (doesn't work, why?)
    implementation("io.confluent", "kafka-json-schema-serializer", "7.6.6")
    implementation("io.confluent", "kafka-avro-serializer", "7.6.6")

    // For code inspection only:
    testImplementation("io.confluent", "kafka-connect-json-schema-converter", "7.6.6")
    testImplementation("io.confluent", "kafka-connect-avro-converter", "7.6.6")
}

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_11)
    }
}

tasks.compileJava {
    sourceCompatibility = "11"
    targetCompatibility = "11"
}

tasks.shadowJar {
    transform(DataReaderTransformer())
    mergeServiceFiles()
    archiveFileName.set("xtdb-kafka-connect.jar")
}
