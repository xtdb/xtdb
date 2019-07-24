./dev/shadow-prod.sh
./bin/capsule -m edge.main -A:prod ebs/project.jar && cd ebs && zip foo.zip project.jar && zip -r foo.zip .ebextensions
