FROM 955308952094.dkr.ecr.eu-west-2.amazonaws.com/crux-bench:base

WORKDIR /usr/local/lib/crux

CMD git pull && \
  lein sub -s "crux-core/:crux-kafka/:crux-kafka-embedded/:crux-rocksdb/:crux-bench/" install && \
  cd crux-bench/ && \
  lein run
