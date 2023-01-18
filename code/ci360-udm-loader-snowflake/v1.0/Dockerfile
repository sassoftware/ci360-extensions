FROM python:3.8-slim-buster

RUN apt-get update && apt-get install -y \
procps git

WORKDIR /app
COPY config/ ./config/
COPY scr/ ./scr/

RUN git clone https://github.com/sassoftware/ci360-download-client-python

RUN cat /etc/debian_version
RUN python --version

RUN python -m pip install --upgrade pip
RUN pip install argparse backoff pybase64 pandas pyJWT requests tqdm cryptography uuid

RUN apt install s3fs vim -y

RUN chmod 600 ./config/.passwd-s3fs
RUN chmod 600 ./config/.passwd-snowflake
RUN chmod 600 ./config/.snowflake_rsa_private_key.pem

WORKDIR /app/scr

ENTRYPOINT [ "/bin/bash", "-l", "-c" ]
