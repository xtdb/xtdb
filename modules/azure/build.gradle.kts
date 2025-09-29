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
            name.set("XTDB Azure")
            description.set("XTDB Azure")
        }
    }
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(24))

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    api("com.azure", "azure-storage-blob", "12.27.0")
    api("com.azure", "azure-identity", "1.13.1")
    api("com.azure", "azure-core-management", "1.15.1")

    // metrics
    api(libs.micrometer.registry.azuremonitor)

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
