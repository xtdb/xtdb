plugins {
    `java-library`
    alias(libs.plugins.clojurephant)
    `maven-publish`
    signing
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB Main")
            description.set("XTDB Main - Entry point and logging configuration")
        }
    }
}

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    api(libs.clojure)
    api(libs.clojure.tools.cli)
    api(libs.clojure.tools.logging)
    api(libs.slf4j.api)

    // Logback for logging implementation
    implementation(libs.logback.classic)

    testImplementation(testFixtures(project(":")))
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

tasks.javadoc.get().enabled = false
