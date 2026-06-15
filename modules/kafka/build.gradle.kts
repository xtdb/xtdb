import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    `java-library`
    alias(libs.plugins.clojurephant)
    `maven-publish`
    signing
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.dokka)

    alias(libs.plugins.protobuf)
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB Kafka")
            description.set("XTDB Kafka")
        }
    }
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    api(libs.kafka.clients)

    api(kotlin("stdlib"))

    api(libs.protobuf.kotlin)
    api(libs.kotlinx.serialization.json)

    // Kafka Connect external source
    api(libs.kafka.connect.api)
    api(libs.kafka.connect.transforms)

    // Converters are loaded reflectively from user config, never referenced in source.
    // Bundle the two common ones so they work out of the box; operators wanting others
    // (e.g. JsonSchema) add the converter jar to their own classpath.
    runtimeOnly(libs.kafka.connect.json)
    runtimeOnly(libs.kafka.connect.avro.converter)

    implementation(libs.kotlinx.coroutines)

    testImplementation(libs.mockk)
    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.kafka.avro.serializer)
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
