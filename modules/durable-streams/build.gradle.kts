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
            name.set("XTDB Durable Streams")
            description.set("XTDB Durable Streams log module")
        }
    }
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    // protobuf-kotlin is required for the Log.Registration interface signature
    // (fromProto takes com.google.protobuf.Any).
    api(libs.protobuf.kotlin)

    api(kotlin("stdlib"))

    implementation(libs.kotlinx.coroutines)

    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
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
