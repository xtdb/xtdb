plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
    kotlin("jvm")
    kotlin("plugin.serialization")
    id("org.jetbrains.dokka")

    alias(libs.plugins.protobuf)
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB AWS")
            description.set("XTDB AWS")
        }
    }
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    api("software.amazon.awssdk", "s3", "2.25.50")

    // metrics
    api(libs.micrometer.registry.cloudwatch2)
    api("software.amazon.awssdk", "cloudwatch", "2.25.50")
    api("software.amazon.awssdk", "sts", "2.25.50")

    api(kotlin("stdlib"))

    api(libs.protobuf.kotlin)

    api(libs.kotlinx.coroutines)
    testImplementation(libs.kotlinx.coroutines.test)
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
