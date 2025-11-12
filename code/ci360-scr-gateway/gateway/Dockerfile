# --- Build stage ---
FROM python:3.11-slim

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

COPY ./app/. /app

EXPOSE 80
COPY --chmod=775 entrypoint.sh /entrypoint.sh
ENTRYPOINT [ "/entrypoint.sh" ] 

#CMD tail -f /dev/null
