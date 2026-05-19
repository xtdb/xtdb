plugins {
    `java-library`
    alias(libs.plugins.clojurephant)
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.protobuf)
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    api(kotlin("stdlib"))
    api(libs.kotlinx.serialization.json)
    api(libs.protobuf.kotlin)

    implementation(libs.pgjdbc)
    implementation(libs.jdbi.core)
    implementation(libs.jdbi.kotlin)

    testImplementation(project(":modules:xtdb-kafka"))
    testImplementation(project(":modules:xtdb-aws"))
    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.minio)
    testImplementation(libs.minio)
    testImplementation(libs.pgjdbc)
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${libs.versions.protobuf.asProvider().get()}"
    }

    generateProtoTasks {
        all().forEach {
            it.builtins {
                create("kotlin")
            }
        }
    }
}
