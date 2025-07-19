import dev.clojurephant.plugin.clojure.tasks.ClojureCompile
import org.jetbrains.dokka.base.DokkaBase
import org.jetbrains.dokka.base.DokkaBaseConfiguration
import org.jreleaser.gradle.plugin.dsl.deploy.maven.MavenDeployer
import org.jreleaser.gradle.plugin.tasks.JReleaserDeployTask
import org.jreleaser.model.Active
import org.jreleaser.model.api.deploy.maven.MavenCentralMavenDeployer
import java.time.Year

evaluationDependsOnChildren()

buildscript {
    dependencies {
        classpath("org.jetbrains.dokka:dokka-base:1.9.10")
    }
}

plugins {
    `java-library`
    id("dev.clojurephant.clojure") version "0.8.0"
    id("io.freefair.aggregate-javadoc-legacy") version "8.13.1"
    kotlin("jvm")
    kotlin("plugin.serialization")
    id("org.jetbrains.dokka")
    alias(libs.plugins.jreleaser)
}

val defaultJvmArgs = listOf(
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "-Dio.netty.tryReflectionSetAccessible=true",
    "-Djdk.attach.allowAttachSelf",
    "-Darrow.memory.debug.allocator=false",
    "-XX:-OmitStackTraceInFastThrow",
    "-Dlogback.configurationFile=${rootDir.resolve("src/main/resources/logback-test.xml")}",
    "-Dxtdb.rootDir=$rootDir",
    "-Djunit.jupiter.extensions.autodetection.enabled=true"
)

val sixGBJvmArgs = listOf(
    "-Xmx2g",
    "-Xms2g",
    "-XX:MaxDirectMemorySize=3g",
    "-XX:MaxMetaspaceSize=1g"
)

val twelveGBJvmArgs = listOf(
    "-Xmx5g",
    "-Xms5g",
    "-XX:MaxDirectMemorySize=6g",
    "-XX:MaxMetaspaceSize=1g"
)

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

val buildEnv = "standard"

layout.buildDirectory.set(
    when (buildEnv) {
        "repl" -> layout.projectDirectory.dir("buildRepl")
        else -> layout.projectDirectory.dir("build")
    }
)

val rootProj = project

allprojects {
    val proj = this

    // labs subprojects set this explicitly - this runs afterwards
    group = if (proj.hasProperty("labs")) "com.xtdb.labs" else "com.xtdb"

    version = System.getenv("XTDB_VERSION") ?: "2.0.0-SNAPSHOT"

    repositories {
        mavenCentral()
        maven { url = uri("https://repo.clojars.org/") }
    }

    if (plugins.hasPlugin("java-library")) {
        java {
            withSourcesJar()
        }

        tasks.jar {
            manifest {
                attributes(
                    "Implementation-Version" to project.version,
                )
            }
        }

        if (plugins.hasPlugin("org.jetbrains.dokka"))
            tasks.register<Jar>("dokkaJavadocJar") {
                dependsOn(tasks.dokkaJavadoc)
                from(tasks.dokkaJavadoc.flatMap { it.outputDirectory })
                archiveClassifier.set("javadoc")
            }

        tasks.test {
            jvmArgs(defaultJvmArgs + sixGBJvmArgs)
            // To stub an AWS region
            environment("AWS_REGION", "eu-west-1")
            useJUnitPlatform {
                excludeTags("integration", "jdbc", "timescale", "s3", "minio", "slt", "docker", "azure", "google-cloud")
            }

            /*
              // this one logs every test start/finish - useful if there's a hanging test
              testLogging {
                  events("started", "passed", "skipped", "failed")
              }
            */

        }

        tasks.register("integration-test", Test::class) {
            jvmArgs(defaultJvmArgs + twelveGBJvmArgs)
            useJUnitPlatform {
                includeTags("integration")
            }
        }

        tasks.register("nightly-test", Test::class) {
            jvmArgs(defaultJvmArgs + sixGBJvmArgs)
            useJUnitPlatform {
                includeTags("s3", "google-cloud", "azure")
            }
        }

        dependencies {
            testRuntimeOnly(libs.logback.classic)
            testRuntimeOnly(libs.logback.classic)
            devRuntimeOnly(libs.slf4j.jpl)
            testRuntimeOnly(libs.slf4j.jpl)

            testImplementation(libs.junit.jupiter.api)
            testRuntimeOnly(libs.junit.jupiter.engine)
            testRuntimeOnly(libs.junit.platform.launcher)
            testImplementation(libs.testcontainers)
            testImplementation(libs.testcontainers.kafka)
            testImplementation(libs.testcontainers.minio)
        }

        if (plugins.hasPlugin("dev.clojurephant.clojure")) {
            dependencies {
                implementation(libs.clojure)

                testRuntimeOnly("dev.clojurephant", "jovial", "0.4.1")
                nrepl("cider", "cider-nrepl", "0.50.1")
            }

            clojure {
                // disable `check` because it takes ages to start a REPL
                builds.forEach {
                    it.checkNamespaces.empty()
                }
            }

            tasks.clojureRepl {
                doFirst {
                    project.ext.set("buildEnv", "repl")
                }

                forkOptions.run {
                    val jvmArgs = defaultJvmArgs.toMutableList()

                    // memoryMaximumSize = "4g"

                    if (project.hasProperty("yourkit")) {
                        jvmArgs += "-agentpath:/opt/yourkit/bin/linux-x86-64/libyjpagent.so=app_name=xtdb"
                    }

                    if (project.hasProperty("debugJvm")) {
                        jvmArgs += "-Xdebug"
                        jvmArgs += "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"
                    }

                    if (project.hasProperty("noLocalsClearing")) {
                        jvmArgs += "-Dclojure.compiler.disable-locals-clearing=true"
                    }

                    if (project.hasProperty("arrowUnsafeMemoryAccess")) {
                        jvmArgs += "-Darrow.enable_unsafe_memory_access=true"
                    }

                    if (project.hasProperty("enableAssertions")) {
                        jvmArgs += "-enableassertions"
                    }

                    if (project.hasProperty("twelveGBJvm")) {
                        jvmArgs += twelveGBJvmArgs
                    }

                    this.jvmArgs = jvmArgs
                }

                middleware.add("cider.nrepl/cider-middleware")
            }

            tasks.withType(ClojureCompile::class) {
                forkOptions.run {
                    jvmArgs = defaultJvmArgs
                }
            }
        }

        // apply 'maven-publish' in the sub-module to bring in all of this shared config.
        // apparently the Done Thing™ is now to create your own plugin - no doubt we'll migrate at some point.
        if (plugins.hasPlugin("maven-publish")) {
            extensions.configure(PublishingExtension::class) {
                publications.named("maven", MavenPublication::class) {
                    from(components["java"])

                    if (plugins.hasPlugin("org.jetbrains.dokka"))
                        artifact(tasks["dokkaJavadocJar"]) {
                            this.classifier = "javadoc"
                        }

                    pom {
                        url = "https://xtdb.com"

                        licenses {
                            license {
                                name = "The MPL License"
                                url = "https://opensource.org/license/mpl-2-0/"
                            }
                        }
                        developers {
                            developer {
                                id = "juxt"
                                name = "JUXT"
                                email = "hello@xtdb.com"
                            }
                        }
                        scm {
                            connection.set("scm:git:git://github.com/xtdb/xtdb.git")
                            developerConnection.set("scm:git:ssh://github.com/xtdb/xtdb.git")
                            url.set("https://xtdb.com")
                        }
                    }

                    // see https://docs.gradle.org/current/userguide/publishing_maven.html#publishing_maven:resolved_dependencies
                    versionMapping {
                        usage("java-api") {
                            fromResolutionOf("runtimeClasspath")
                        }
                        usage("java-runtime") {
                            fromResolutionResult()
                        }
                    }
                }

                // we 'publish' these to a local, on-disk Maven repo, so that JReleaser has
                // something to upload
                repositories {
                    maven {
                        name = "jreleaserStaging"
                        url = uri(layout.buildDirectory.dir("jreleaser/maven-staging"))
                    }
                }

                // we clean out the local Maven staging repo before we do the local publication
                tasks.register<Delete>("cleanJreleaserStagingRepository") {
                    delete(layout.buildDirectory.dir("jreleaser/maven-staging"))
                }

                tasks.named("publishMavenPublicationToJreleaserStagingRepository") {
                    dependsOn("cleanJreleaserStagingRepository")
                }

                // signing is done through the `gpg` command on your machine.
                extensions.configure(SigningExtension::class) {
                    useGpgCmd()
                    sign(publications["maven"])
                }
            }
        }
    }
}

val jreleaserDeployTask = tasks.getByName<JReleaserDeployTask>("jreleaserDeploy")

jreleaser {
    strict = true

    deploy.active = Active.ALWAYS
    deploy.maven {
        fun MavenDeployer.setup() {
            // set these up in your `~/.gradle/gradle.properties`
            username = properties["centralUsername"] as? String
            password = properties["centralPassword"] as? String

            rootProj.allprojects {
                val proj = this

                // we use the on-disk Maven repos created by the maven-publish plugin.
                // we combine them all into one bundle to upload to Central
                if (proj.plugins.hasPlugin("maven-publish")) {
                    jreleaserDeployTask.dependsOn(
                        proj.tasks
                            .getByName("publishMavenPublicationToJreleaserStagingRepository")
                    )

                    stagingRepositories.add(
                        proj.layout.buildDirectory
                            .dir("jreleaser/maven-staging")
                            .get().toString()
                    )
                }
            }

            sign = false // already done in maven-publish
        }

        // release side is through the new Maven Central 'Publisher' API
        // https://jreleaser.org/guide/latest/reference/deploy/maven/maven-central.html
        mavenCentral.create("release") {
            active = Active.RELEASE_PRERELEASE
            url = "https://central.sonatype.com/api/v1/publisher"

            setup()

            // this only uploads the artifacts. we could get it to publish them as well,
            // but I quite like having a final 'big red button' step.
            // https://central.sonatype.com/publishing
            stage = MavenCentralMavenDeployer.Stage.UPLOAD
        }

        // main Publisher API doesn't support snapshots, so we use a more standard 'nexus2' repository.
        // https://jreleaser.org/guide/latest/reference/deploy/maven/nexus2.html
        nexus2.create("snapshots") {
            active = Active.SNAPSHOT
            url = "https://central.sonatype.com/repository/maven-snapshots/"
            snapshotUrl = "https://central.sonatype.com/repository/maven-snapshots/"
            snapshotSupported = true

            setup()
        }
    }
}

project(":xtdb-core").run {
    tasks["sourcesJar"].dependsOn("generateGrammarSource")
    tasks["dokkaJavadoc"].dependsOn("generateGrammarSource")
    tasks["dokkaHtmlPartial"].dependsOn("generateGrammarSource")
}

dependencies {
    fun projectDep(name: String) {
        testImplementation(project(name))
        val mainSourceSet = project(name).dependencyProject.sourceSets.main.get()
        devImplementation(mainSourceSet.clojure.sourceDirectories)
        devImplementation(mainSourceSet.resources.sourceDirectories)
    }

    projectDep(":xtdb-api")
    projectDep(":xtdb-core")

    projectDep(":xtdb-http-server")

    projectDep(":modules:xtdb-kafka")
    projectDep(":modules:xtdb-aws")
    projectDep(":modules:xtdb-azure")
    projectDep(":modules:xtdb-google-cloud")

    projectDep(":modules:bench")
    projectDep(":modules:xtdb-datasets")
    projectDep(":modules:xtdb-flight-sql")
    projectDep(":modules:xtdb-kafka-connect")
    projectDep(":cloud-benchmark")

    api(libs.slf4j.api)
    api(libs.logback.classic)
    api(libs.clojure.tools.logging)

    api(libs.next.jdbc)
    testImplementation(libs.honeysql)
    api(libs.integrant)
    api(project(":xtdb-core"))

    api(libs.pgjdbc)
    testImplementation(libs.exposed.core)
    testImplementation(libs.exposed.jdbc)
    testImplementation(libs.exposed.java.time)

    implementation(libs.junit.jupiter.api)

    testImplementation(libs.clojure.`data`.csv)
    testImplementation(libs.clojure.tools.cli)


    devImplementation("integrant", "repl", "0.3.2")
    devImplementation("com.azure", "azure-identity", "1.9.0")
    devImplementation("com.taoensso", "tufte", "2.6.3")
    devImplementation("clojure.java-time:clojure.java-time:1.4.3")
    testImplementation("metosin", "jsonista", "0.3.3")
    testImplementation("clj-commons", "clj-yaml", "1.0.27")
    testImplementation("org.xerial", "sqlite-jdbc", "3.39.3.0")
    testImplementation("org.clojure", "test.check", "1.1.1")
    testImplementation("clj-kondo", "clj-kondo", "2023.12.15")
    testImplementation("com.github.igrishaev", "pg2-core", "0.1.33")
    testImplementation(libs.hato)

    // For generating clojure docs
    testImplementation("codox", "codox", "0.10.8")

    implementation(libs.kotlinx.coroutines)
    testImplementation(libs.kotlinx.coroutines.test)
    implementation(kotlin("stdlib-jdk8"))
    testImplementation(kotlin("test"))
    testImplementation(kotlin("test-junit5"))

    // for AWS profiles (managing datasets)
    devImplementation("software.amazon.awssdk", "sts", "2.16.76")
    devImplementation("software.amazon.awssdk", "sso", "2.16.76")
    devImplementation("software.amazon.awssdk", "ssooidc", "2.16.76")

    testImplementation("org.testcontainers", "minio", "1.20.0")
    testImplementation("io.minio", "minio", "8.5.17")

    devImplementation("com.taoensso", "nippy", "3.3.0")
    testImplementation("com.taoensso", "nippy", "3.3.0")

    // hato uses cheshire for application/json encoding
    testImplementation("cheshire", "cheshire", "5.12.0")
}

if (hasProperty("fin")) {
    dependencies {
        devImplementation("vvvvalvalval", "scope-capture", "0.3.3")
        devImplementation("lambdaisland", "deep-diff2", "2.12.219")
    }
}

val codoxOpts = File("${projectDir}/codox.edn").readText()

tasks.register("build-codox", JavaExec::class) {
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass.set("clojure.main")
    jvmArgs(defaultJvmArgs + sixGBJvmArgs)
    val args = mutableListOf(
        "-e", "(require 'codox.main)",
        "-e", "(codox.main/generate-docs ${codoxOpts})"
    )

    this.args = args
}

fun createSltTask(
    taskName: String,
    maxFailures: Long = 0,
    maxErrors: Long = 0,
    testFiles: List<String> = emptyList(),
    extraArgs: List<String> = emptyList(),
) {
    tasks.register(taskName, JavaExec::class) {
        classpath = sourceSets.test.get().runtimeClasspath
        mainClass.set("clojure.main")
        jvmArgs(defaultJvmArgs + sixGBJvmArgs)
        this.args = listOf(
            "-m", "xtdb.sql.logic-test.runner",
            "--verify",
            "--db", "xtdb",
            "--max-failures", maxFailures.toString(),
            "--max-errors", maxErrors.toString(),
        ) + extraArgs + testFiles.map {
            "src/test/resources/xtdb/sql/logic_test/sqlite_test/$it"
        }
    }
}

createSltTask(
    "slt-test",
    maxFailures = 295,
    maxErrors = 4,
    testFiles = listOf(
        "xtdb.test",
        "select1.test", "select2.test", "select3.test", "select4.test",
        // "select5.test",
        "random/expr/slt_good_0.test",
        "random/aggregates/slt_good_0.test",
        "random/groupby/slt_good_0.test",
        "random/select/slt_good_0.test"
    )
)

createSltTask(
    "slt-test-2",
    maxFailures = 10,
    testFiles = listOf(
        "index/between/1/slt_good_0.test",
        "index/commute/10/slt_good_0.test",
        "index/in/10/slt_good_0.test",
        "index/orderby/10/slt_good_0.test",
        "index/orderby_nosort/10/slt_good_0.test",
        "index/random/10/slt_good_0.test",
    )
)

createSltTask(
    "slt-test-dir",
    maxFailures = if (project.hasProperty("testMaxFailures")) {
        val testMaxFailures: String by project
        if (testMaxFailures.isEmpty()) 0 else testMaxFailures.toLong()
    } else 0,

    maxErrors = if (project.hasProperty("testMaxErrors")) {
        val testMaxErrors: String by project
        if (testMaxErrors.isEmpty()) 0 else testMaxErrors.toLong()
    } else 0,

    testFiles = if (project.hasProperty("testDir")) {
        val testDir: String by project
        listOf(testDir)
    } else emptyList(),

    extraArgs = listOf("--dirs")
)

fun createBench(benchName: String, properties: Map<String, String>) {
    tasks.register(benchName, JavaExec::class) {
        dependsOn(":modules:bench:assemble")
        classpath = sourceSets.dev.get().runtimeClasspath
        mainClass.set("clojure.main")
        jvmArgs(defaultJvmArgs + sixGBJvmArgs + listOf("-Darrow.enable_unsafe_memory_access=true"))
        val args = mutableListOf("-m", "xtdb.bench", benchName)

        val extraProps = properties + mapOf(
            "dir" to "--node-dir",
            "configFile" to "--config-file"
        )

        extraProps.forEach { (k, v) ->
            if (project.hasProperty(k)) {
                args.add(v)
                args.add(project.properties[k] as String)
            }
        }

        if (project.hasProperty("yourkit"))
            jvmArgs("-agentpath:/opt/yourkit/bin/linux-x86-64/libyjpagent.so=on_exit=snapshot,async_sampling_cpu,app_name=xtdb-$benchName")

        this.args = args
    }
}

createBench("tpch", mapOf("scaleFactor" to "--scale-factor"))

createBench(
    "auctionmark",
    mapOf(
        "scaleFactor" to "--scale-factor",
        "duration" to "--duration",
        "threads" to "--threads"
    )
)

createBench("products", mapOf("limit" to "--limit"))

// can't seem to have an arg with the same name as a task
createBench("readings", mapOf("readingCount" to "--readings", "deviceCount" to "--devices"))

createBench("tsbs-iot", mapOf("file" to "--file"))

tasks.dokkaHtmlMultiModule {
    moduleName.set("")
    moduleVersion.set("2.0.0-SNAPSHOT")

    inputs.file("dokka/logo-styles.css")
    inputs.file("dokka/logo-icon.svg")

    pluginConfiguration<DokkaBase, DokkaBaseConfiguration> {
        customAssets = listOf(file("dokka/logo-icon.svg"))
        customStyleSheets = listOf(file("dokka/logo-styles.css"))

        footerMessage = "© ${Year.now().value} JUXT Ltd"
    }
}
