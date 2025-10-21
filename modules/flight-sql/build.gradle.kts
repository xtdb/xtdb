import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    `java-library`
    alias(libs.plugins.clojurephant)
    `maven-publish`
    signing
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.dokka)
}

ext {
    set("labs", true)
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB FlightSQL Server")
            description.set("XTDB FlightSQL Server")
        }
    }
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    api("org.apache.arrow", "arrow-vector", "15.0.2")
    api("org.apache.arrow", "flight-sql", "15.0.2")

    api(kotlin("stdlib"))

    testImplementation(testFixtures(project(":")))

    // brings in vendored SLF4J (but doesn't change the class names). naughty.
    // https://github.com/apache/arrow/issues/34516
    testImplementation("org.apache.arrow", "flight-sql-jdbc-driver", "15.0.2")

    testImplementation("com.github.seancorfield", "next.jdbc", "1.3.939")

    devImplementation(sourceSets.main.get().output)

    testImplementation(libs.arrow.adbc)
    testImplementation(libs.arrow.adbc.fsql)
}
