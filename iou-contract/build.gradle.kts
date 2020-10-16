plugins {
    kotlin("jvm")
    id("net.corda.plugins.cordapp")
}

val cordaGroup = "net.corda"
val cordaVersion = "4.5"

cordapp {
    contract {
        targetPlatformVersion = 4
        minimumPlatformVersion = 4
    }
}

dependencies {
    cordaCompile(cordaGroup, "corda-core", cordaVersion)
    implementation(project(":crux-corda-state"))
}
