plugins {
    java // for JavaToolchainService
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

// From jdeps analysis of the standalone uber-jar, plus manual additions.
// To re-run: ./gradlew :docker:standalone:shadowJar
//   jdeps --ignore-missing-deps --print-module-deps docker/standalone/build/libs/xtdb-standalone.jar
val jlinkModules = listOf(
    // from jdeps
    "java.base", "java.compiler", "java.desktop", "java.instrument",
    "java.prefs", "java.security.jgss", "java.sql",
    "jdk.jdi", "jdk.management", "jdk.unsupported",

    // manual additions — invisible to jdeps static analysis
    "java.logging",   // System.Logger in Kotlin (SystemLogger.kt, Azure module)
    "java.naming",    // JNDI (transitive dep of various libs)
    "java.net.http",  // Hato HTTP client (xtdb.authn OAuth/OIDC)
    "jdk.compiler",   // codox on the REPL/dev classpath
    "jdk.javadoc",    // codox on the REPL/dev classpath
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
