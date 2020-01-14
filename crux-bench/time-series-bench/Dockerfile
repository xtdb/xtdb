FROM 955308952094.dkr.ecr.eu-west-2.amazonaws.com/crux-bench:base

CMD cd crux/ && git pull && lein sub install && cd ../ && lein run

ADD project.clj .
ADD src/ ./src/
