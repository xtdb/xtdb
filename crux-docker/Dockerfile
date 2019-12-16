FROM clojure:openjdk-11-tools-deps

VOLUME /root/.m2
VOLUME /var/lib/crux

EXPOSE 3000

WORKDIR /usr/lib/crux

CMD ["clojure", "-m", "crux.docker"]

ADD deps.edn .
ADD src/ ./src/