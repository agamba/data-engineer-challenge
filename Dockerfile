# smaller image
FROM python:3.9-slim-buster
# FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

# set these vars in GCP for secutiry
# ENV DB_HOST=
# ENV DB_NAME=
# ENV DB_USER=
# ENV DB_PASS=
# ENV DB_PORT=
# ENV DB_SOCKET_DIR=/cloudsql
# ENV INSTANCE_CONNECTION_NAME=

CMD exec gunicorn --bind :8080 --workers 1 --threads 8 --timeout 0 app:app
