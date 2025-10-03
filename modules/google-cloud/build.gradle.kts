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

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB Google Cloud")
            description.set("XTDB Google Cloud")
        }
    }
}

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    api("com.google.cloud", "google-cloud-storage", "2.38.0") {
        exclude("com.google.guava","listenablefuture")
    }

    api("com.google.guava","guava","32.1.1-jre")

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
