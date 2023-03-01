plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("Core2 Kafka")
            description.set("Core2 Kafka")
        }
    }
}

dependencies {
    api(project(":api"))
    api(project(":core"))

    api("org.apache.kafka", "kafka-clients", "3.1.0")
}
