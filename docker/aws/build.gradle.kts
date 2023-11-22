// Shadowing Test Sources and Dependencies
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    java
    application
    id("com.github.johnrengelman.shadow") version "7.1.2"
}

dependencies {
    implementation(project(":core"))
    implementation(project(":http-server"))
    implementation(project(":modules:kafka"))
    implementation(project(":modules:s3"))
    implementation("ch.qos.logback", "logback-classic", "1.4.5")
}

application {
    mainClass.set("clojure.main")
}

tasks.shadowJar {
    archiveBaseName.set("xtdb")
    archiveVersion.set("")
    archiveClassifier.set("aws")
    mergeServiceFiles()
}