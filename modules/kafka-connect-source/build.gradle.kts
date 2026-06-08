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
    api(project(":modules:xtdb-kafka"))

    api(kotlin("stdlib"))
    api(libs.kotlinx.serialization.json)
    api(libs.protobuf.kotlin)

    implementation(libs.kafka.clients)
    api(libs.kafka.connect.api)
    api(libs.kafka.connect.json)
    api(libs.kafka.connect.transforms)

    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.kafka.connect.avro.converter)
    testImplementation(libs.kafka.json.schema.serializer)
    testImplementation(libs.kafka.connect.json.schema.converter)
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
