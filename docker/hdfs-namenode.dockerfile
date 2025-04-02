ARG BASE_IMAGE=hdfs-base:latest

FROM $BASE_IMAGE

RUN mkdir -p /hadoop/dfs/name

COPY run-namenode.sh /run-namenode.sh

RUN chmod a+x /run-namenode.sh

CMD ["/run-namenode.sh"]

