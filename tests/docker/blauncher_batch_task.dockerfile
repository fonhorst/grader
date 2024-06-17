ARG GEOWSM_REGISTRY_ADDR=10.32.0.243:5000
FROM ${GEOWSM_REGISTRY_ADDR}/geowsm-fcache-rnseism-deps:latest

RUN apt-get update && apt-get install -y nano default-jdk

COPY tests/deploy/job_examples /jobs

ENV PYTHONPATH='/jobs'

WORKDIR /

ENTRYPOINT ["rnblauncher"]
