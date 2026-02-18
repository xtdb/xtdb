rootProject.name = "xtdb"

includeBuild("build-logic/jlink")

include("api", "core", "main")
project(":api").name = "xtdb-api"
project(":core").name = "xtdb-core"
project(":main").name = "xtdb-main"

include("lang:test-harness")
project(":lang:test-harness").name = "test-harness"

include("docker:standalone", "docker:aws", "docker:azure", "docker:google-cloud")

include("modules:kafka", "modules:kafka-connect", "modules:debezium", "modules:aws", "modules:azure", "modules:google-cloud")
project(":modules:kafka").name = "xtdb-kafka"
project(":modules:kafka-connect").name = "xtdb-kafka-connect"
project(":modules:debezium").name = "xtdb-debezium"
project(":modules:aws").name = "xtdb-aws"
project(":modules:azure").name = "xtdb-azure"
project(":modules:google-cloud").name = "xtdb-google-cloud"

include("modules:bench", "modules:datasets")
project(":modules:datasets").name = "xtdb-datasets"

include("monitoring", "monitoring:docker-image")
