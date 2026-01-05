plugins {
    `kotlin-dsl`
    id("com.gradleup.shadow") version "8.3.6"
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    implementation("org.clojure:clojure:1.12.0")
    implementation(gradleApi())
    implementation("com.gradleup.shadow:shadow-gradle-plugin:8.3.6")
    implementation("org.apache.ant:ant:1.10.15") // eugh. _all_ of Ant for the DataReaderTransformer?

    // this one because jreleaser depends on Jackson 2.15+
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.20")
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

tasks.shadowJar {
    isZip64 = true
}
