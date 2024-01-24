plugins {
    `java-library`
    id("dev.clojurephant.clojure")
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(17))

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))

    api("io.airlift.tpch", "tpch", "0.10")
    api("org.clojure", "data.csv", "1.0.1")

    api("software.amazon.awssdk", "s3", "2.16.76")
}
