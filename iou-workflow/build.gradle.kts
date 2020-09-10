plugins {
    kotlin("jvm")
    id("dev.clojurephant.clojure")
    id("net.corda.plugins.cordapp")
    id("net.corda.plugins.quasar-utils")
}

val cordaGroup = "net.corda"
val cordaVersion = "4.5"

cordapp {
    workflow {
        targetPlatformVersion = 4
        minimumPlatformVersion = 4
    }
}

dependencies {
    implementation("org.clojure", "clojure", "1.10.0")
    implementation("juxt", "crux-core", "20.09-1.11.0-beta")

    cordaCompile(cordaGroup, "corda-core", cordaVersion)
    cordaCompile(cordaGroup, "corda-jackson", cordaVersion)
    cordaCompile(cordaGroup, "corda-rpc", cordaVersion)
    cordaRuntime(cordaGroup, "corda", cordaVersion)
    cordapp(project(":iou-contract"))
    cordapp(project(":corda-contract"))
    cordapp(project(":corda-workflow"))

    testImplementation("junit", "junit", "4.12")
    testImplementation(cordaGroup, "corda-node-driver", cordaVersion)
}

