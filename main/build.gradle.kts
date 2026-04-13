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
    implementation(libs.log4j.core)

    runtimeOnly(libs.log4j.core)
    runtimeOnly(libs.log4j.jpl)
    runtimeOnly(libs.log4j.jul)
    runtimeOnly(libs.log4j.jcl)
    runtimeOnly(libs.log4j.slf4j2.impl)

    testImplementation(testFixtures(project(":")))
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

tasks.javadoc.get().enabled = false
