plugins {
    `kotlin-dsl`
    id("com.gradleup.shadow") version "8.3.5"
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    implementation("org.clojure", "clojure", "1.12.0")
    implementation(gradleApi())
    implementation("com.gradleup.shadow:shadow-gradle-plugin:8.3.5")
    implementation("org.apache.ant:ant:1.10.13") // eugh. _all_ of Ant for the DataReaderTransformer?
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

tasks.shadowJar {
    isZip64 = true
}
