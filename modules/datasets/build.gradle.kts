plugins {
    `java-library`
    id("dev.clojurephant.clojure")

    `maven-publish`
    signing
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":"))
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))
    api(project(":xtdb-jdbc"))

    api("io.airlift.tpch", "tpch", "0.10")
    api("org.clojure", "data.csv", "1.0.1")

    api("software.amazon.awssdk", "s3", "2.16.76")
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
