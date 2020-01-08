FROM clojure:openjdk-11-lein-2.9.0

RUN apt-get update
RUN apt-get install -y awscli

COPY bin/download_watdiv_files.sh /bin/download_watdiv_files.sh
WORKDIR /watdiv-data
RUN /bin/download_watdiv_files.sh
