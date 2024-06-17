FROM spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root

RUN apt-get update && apt-get install -y git nano net-tools

COPY requirements_base.txt /tmp/

RUN pip3 install -r /tmp/requirements_base.txt

COPY requirements_rn.txt /tmp/

RUN pip3 install -r /tmp/requirements_rn.txt


ENV CXX_CLICKHOUSE_VERSION "0.0.13"

COPY libs/cxx_clickhouse/whl/cxx_clickhouse-${CXX_CLICKHOUSE_VERSION}-cp310-none-manylinux_2_35_x86_64.whl /tmp

RUN pip3 install /tmp/cxx_clickhouse-${CXX_CLICKHOUSE_VERSION}-cp310-none-manylinux_2_35_x86_64.whl

ENV CXX_COMMON_VERSION "0.0.2"

COPY libs/cxx_common/whl/cxx_common-${CXX_COMMON_VERSION}-cp310-none-manylinux_2_35_x86_64.whl /tmp

RUN pip3 install /tmp/cxx_common-${CXX_COMMON_VERSION}-cp310-none-manylinux_2_35_x86_64.whl

ENV GEOWSM_VERSION "0.1.2"

COPY dist/geowsm-${GEOWSM_VERSION}-py3-none-any.whl /tmp

RUN pip3 install /tmp/geowsm-${GEOWSM_VERSION}-py3-none-any.whl

USER spark

WORKDIR /opt/spark/work-dir

ENTRYPOINT [ "/opt/entrypoint.sh" ]
