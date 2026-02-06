plugins {
    java
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

// To re-run:
// 1) ./gradlew :docker:standalone:shadowJar
// 2) jdeps --ignore-missing-deps --print-module-deps docker/standalone/build/libs/xtdb-standalone.jar
val jlinkModules = listOf(
    // from jdeps
    "java.base", "java.compiler", "java.desktop", "java.instrument",
    "java.naming", "java.prefs", "java.security.jgss", "java.sql",
    "jdk.jdi", "jdk.management", "jdk.unsupported",

    // manual additions
    "java.logging",
    "java.net.http",
)

val customJreDir = layout.buildDirectory.dir("custom-jre")

val toolchainLauncher = extensions.getByType(JavaToolchainService::class.java)
    .launcherFor(java.toolchain)

tasks.register<Exec>("buildCustomJre") {
    description = "Build a custom JRE using jlink"
    val outputDir = customJreDir.get().asFile
    val jlinkPath = toolchainLauncher.map {
        it.metadata.installationPath.file("bin/jlink").asFile.absolutePath
    }

    inputs.property("modules", jlinkModules.joinToString(","))
    inputs.property("jlinkPath", jlinkPath)
    outputs.dir(outputDir)

    doFirst { outputDir.deleteRecursively() }

    commandLine(
        jlinkPath.get(),
        "--add-modules", jlinkModules.joinToString(","),
        "--strip-debug", "--no-man-pages", "--no-header-files",
        "--compress=zip-6",
        "--output", outputDir.absolutePath
    )
}
