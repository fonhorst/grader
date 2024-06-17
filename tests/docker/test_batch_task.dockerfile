FROM busybox:1.36.1-glibc

ENV SLEEP_TIME="5"
ENV EXIT_CODE="0"

ENTRYPOINT /bin/sh -c 'sleep ${SLEEP_TIME} && exit ${EXIT_CODE}'
