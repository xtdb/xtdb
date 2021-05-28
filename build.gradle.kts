import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.4.21"
    `java-library`
    `maven-publish`
}

group = "juxt"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
    maven {
        name = "Clojars"
        url = uri("https://repo.clojars.org/")
    }
}

dependencies {
    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    // TODO: I'm not sure if we need both `implementation` and `api`? -sd
    implementation("juxt:crux-core:21.04-1.16.0-beta")
    api("juxt:crux-core:21.04-1.16.0-beta")

    testImplementation(kotlin("test-junit5"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.6.0")
    testImplementation("com.natpryce:hamkrest:1.8.0.1")
    testImplementation("juxt:crux-rocksdb:21.04-1.16.0-beta")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.6.0")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}