import org.jetbrains.kotlin.gradle.dsl.JvmDefaultMode
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    `java-library`
    alias(libs.plugins.clojurephant)
    `maven-publish`
    signing
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.dokka)
    alias(libs.plugins.dokka.javadoc)

    alias(libs.plugins.protobuf)
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB API")
            description.set("XTDB API")
        }
    }
}

dependencies {
    compileOnlyApi(files("src/main/resources"))
    api(libs.clojure.spec)

    api(libs.transit.clj)
    api(libs.transit.java)

    api(libs.arrow.algorithm)
    api(libs.arrow.compression)
    api(libs.arrow.vector)
    api(libs.arrow.memory.netty)

    api(kotlin("stdlib"))
    api(libs.kotlinx.serialization.json)
    
    api(libs.protobuf.kotlin)

    api(libs.caffeine)

    implementation(libs.pgjdbc)

    testImplementation(libs.next.jdbc)
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

tasks.javadoc {
    exclude("xtdb/util/*")
}

tasks.compileJava {
    sourceCompatibility = "21"
    targetCompatibility = "21"
}

tasks.compileTestJava {
    sourceCompatibility = "21"
    targetCompatibility = "21"
}

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_21)

        jvmDefault.set(JvmDefaultMode.NO_COMPATIBILITY)
    }
}

dokka {
    moduleName.set("xtdb-api")

    dokkaSourceSets.named("main") {
        // Scope the published docs to the packages we intend to expose; everything else
        // (xtdb.types, xtdb.time, xtdb.util interop glue, etc.) is internal by convention.
        perPackageOption {
            matchingRegex.set(".*")
            suppress.set(true)
        }
        perPackageOption {
            matchingRegex.set("xtdb\\.api.*")
            suppress.set(false)
        }
        // xtdb.arrow holds the VectorType lattice; xtdb.arrow.extensions (internal Arrow codec
        // machinery) is matched only by this exact regex, so it stays suppressed by the catch-all.
        perPackageOption {
            matchingRegex.set("xtdb\\.arrow")
            suppress.set(false)
        }
        perPackageOption {
            matchingRegex.set("xtdb\\.jdbc.*")
            suppress.set(false)
        }
        // types/time are mostly internal, but a few value types (Oid, RegClass, RegProc,
        // ZonedDateTimeRange, Interval) surface in public signatures; the rest carry `@suppress`.
        perPackageOption {
            matchingRegex.set("xtdb\\.types.*")
            suppress.set(false)
        }
        perPackageOption {
            matchingRegex.set("xtdb\\.time.*")
            suppress.set(false)
        }
        // xtdb.time exposes the Interval value type; Time.kt is internal parsing/conversion helpers.
        suppressedFiles.from(file("src/main/kotlin/xtdb/time/Time.kt"))
    }
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
