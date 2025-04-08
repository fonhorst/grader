FROM python:3.12-bookworm as builder

RUN pip install poetry

WORKDIR /build

# Copy only the files needed for building
COPY pyproject.toml poetry.lock ./
COPY grader ./grader

# Build wheel
RUN poetry build

# Export requirements
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes

FROM python:3.12-bookworm as final

RUN apt-get update && apt-get install -y \
    curl \
    wget \
    nano \
    git \
    iputils-ping \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy artifacts from builder
COPY --from=builder /build/requirements.txt ./
RUN pip install -r requirements.txt 

# we intentionally keep this section separated to prevent cache invalidation 
# and all dependencies are being re-downloaded and re-installed due to small changes
# in the grader package
COPY --from=builder /build/dist/*.whl ./
RUN pip install /app/*.whl

ENTRYPOINT ["graderctl"]

