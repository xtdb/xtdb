plugins {
    `java-library`
    alias(libs.plugins.clojurephant)

    `maven-publish`
    signing
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":"))
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    api(libs.tpch)
    api(libs.clojure.data.csv)

    api(libs.aws.s3)
    api(libs.next.jdbc)
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB Datasets")
            description.set("XTDB Datasets")
        }
    }
}
