plugins {
    `java-library`
    id("dev.clojurephant.clojure")
}

dependencies {
    api(project(":api"))
    api(project(":core"))

    api("io.airlift.tpch", "tpch", "0.10")
    api("org.clojure", "data.csv", "1.0.1")

    api("software.amazon.awssdk", "s3", "2.16.76")
}
