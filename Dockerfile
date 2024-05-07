FROM python:3.8-slim


# update apt pkgs, and install build-essential for ciso8601
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN python -m pip install stac-fastapi.types stac-fastapi.api stac-fastapi.extensions
RUN pip install xarray[complete] s3fs

# update certs used by Requests
ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -e ./stac_fastapi/demo[dev,server]
