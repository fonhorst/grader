FROM python:3.12-bookworm

RUN apt-get update && apt-get install -y \
    curl \
    wget \
    nano \
    git \
    iputils-ping \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install poetry

RUN mkdir -p /app

COPY requirements.txt /app

RUN pip install -r /app/requirements.txt

ARG GRADER_VERSION=0.1.2

COPY dist/grader-${GRADER_VERSION}-py3-none-any.whl /app

RUN pip install /app/grader-${GRADER_VERSION}-py3-none-any.whl

ENTRYPOINT ["graderctl"]

