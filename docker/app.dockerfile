FROM python:3.12-bookworm AS base

RUN apt-get update && apt-get install -y \
    curl \
    wget \
    nano \
    git \
    iputils-ping \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install poetry

RUN poetry self add poetry-plugin-export

FROM base AS exporter

WORKDIR /build

# Copy only the files needed for building
COPY pyproject.toml poetry.lock ./

# Export requirements
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes

FROM base AS builder

WORKDIR /build

COPY . ./

# Build wheel
RUN poetry build

FROM base AS final

WORKDIR /app

# Copy artifacts from builder
COPY --from=exporter /build/requirements.txt ./
RUN pip install -r requirements.txt 

# we intentionally keep this section separated to prevent cache invalidation 
# and all dependencies are being re-downloaded and re-installed due to small changes
# in the grader package
COPY --from=builder /build/dist/*.whl ./
RUN pip install /app/*.whl

ENTRYPOINT ["graderctl"]

