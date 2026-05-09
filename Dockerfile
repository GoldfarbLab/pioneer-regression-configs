ARG JULIA_VERSION=1.12.6
FROM julia:${JULIA_VERSION}

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
    && ln -sf /usr/bin/python3 /usr/local/bin/python \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
