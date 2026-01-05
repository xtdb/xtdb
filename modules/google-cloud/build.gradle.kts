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

    api(libs.google.cloud.storage) {
        exclude("com.google.guava","listenablefuture")
    }

    api(libs.guava)

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
