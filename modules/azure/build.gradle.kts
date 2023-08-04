plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB Azure")
            description.set("XTDB Azure")
        }
    }
}

dependencies {
    api(project(":api"))
    api(project(":core"))

    api("com.azure", "azure-storage-blob", "12.22.0")
    api("com.azure", "azure-messaging-eventhubs", "5.15.0")
    api("com.azure", "azure-messaging-servicebus", "7.14.2")
    api("com.azure", "azure-identity", "1.9.0")
    api("com.azure", "azure-core-management", "1.11.1")
    api("com.azure.resourcemanager", "azure-resourcemanager-eventhubs", "2.26.0")
}
