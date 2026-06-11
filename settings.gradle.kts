rootProject.name = "xtdb"

includeBuild("build-logic/jlink")

include("api", "core", "main")
project(":api").name = "xtdb-api"
project(":core").name = "xtdb-core"
project(":main").name = "xtdb-main"

if (file("lang/test-harness").isDirectory) {
    include("lang:test-harness")
    project(":lang:test-harness").name = "test-harness"
}

include("docker:standalone", "docker:aws", "docker:azure", "docker:google-cloud")

include("modules:kafka", "modules:kafka-connect", "modules:kafka-connect-source", "modules:postgres-source", "modules:aws", "modules:azure", "modules:google-cloud")
project(":modules:kafka").name = "xtdb-kafka"
project(":modules:kafka-connect").name = "xtdb-kafka-connect"
project(":modules:kafka-connect-source").name = "xtdb-kafka-connect-source"
project(":modules:postgres-source").name = "xtdb-postgres-source"
project(":modules:aws").name = "xtdb-aws"
project(":modules:azure").name = "xtdb-azure"
project(":modules:google-cloud").name = "xtdb-google-cloud"

include("modules:bench", "modules:datasets")
project(":modules:datasets").name = "xtdb-datasets"

// test infra, not published
include("modules:test-watchdog")

include("monitoring")
if (file("monitoring/docker-image").isDirectory) {
    include("monitoring:docker-image")
}
