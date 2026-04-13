import xtdb.DataReaderTransformer

plugins {
    java
    application
    id("com.gradleup.shadow")
}

dependencies {
    implementation(project(":xtdb-main"))
    implementation(project(":modules:xtdb-kafka"))
    implementation(project(":modules:xtdb-aws"))
    implementation(project(":modules:xtdb-debezium"))
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

application {
    mainClass.set("clojure.main")
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Version" to project.version,
        )
    }
}

tasks.shadowJar {
    archiveBaseName.set("xtdb")
    archiveVersion.set("")
    archiveClassifier.set("aws")
    setProperty("zip64", true)
    mergeServiceFiles()
    transform(DataReaderTransformer())
}
