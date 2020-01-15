FROM 955308952094.dkr.ecr.eu-west-2.amazonaws.com/crux-bench:base

WORKDIR /usr/local/lib/crux

ENV GIT_ORIGIN=juxt
ENV GIT_BRANCH=master

CMD git fetch $GIT_ORIGIN && git checkout $GIT_ORIGIN/$GIT_BRANCH && \
  lein sub -s "crux-core/:crux-kafka/:crux-kafka-embedded/:crux-rocksdb/:crux-bench/" install && \
  cd crux-bench/ && \
  lein run
