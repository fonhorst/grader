ARG BASE_IMAGE=hdfs-base:latest

FROM $BASE_IMAGE

ENV JAVA_OPTS="-Dsun.net.inetaddr.ttl=0"

RUN mkdir -p /hadoop/dfs/data

COPY run-datanode.sh /run-datanode.sh

RUN chmod a+x /run-datanode.sh

CMD ["/run-datanode.sh"]

