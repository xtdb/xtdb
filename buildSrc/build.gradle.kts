plugins {
    `kotlin-dsl`
    id("com.github.johnrengelman.shadow") version "7.1.2"
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    implementation("org.clojure", "clojure", "1.11.1")
    implementation(gradleApi())
    implementation("gradle.plugin.com.github.johnrengelman:shadow:7.1.2")
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

tasks.shadowJar {
    isZip64 = true
}
