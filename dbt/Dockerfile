FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
  && rm -rf /var/lib/apt/lists/*

# dbt 1.9.1 (core + postgres aynı minor) + protobuf < 6
RUN python -m pip install --no-cache-dir --upgrade pip \
 && python -m pip install --no-cache-dir \
      "protobuf<6" \
      "dbt-core==1.9.1" \
      "dbt-postgres==1.9.1"

WORKDIR /usr/app
ENV DBT_PROFILES_DIR=/usr/app

CMD ["bash", "-lc", "sleep infinity"]
