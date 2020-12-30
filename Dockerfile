FROM python:3.7
ENV PYTHONUNBUFFERED 1

# Install dockerize https://github.com/jwilder/dockerize
ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

RUN \
	apt-get install -y curl ca-certificates gnupg && \
	curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
	echo "deb http://apt.postgresql.org/pub/repos/apt buster-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
	apt-get update && \
	apt-get install -y postgresql-client-13 && \
	rm -rf /var/lib/apt/lists/*

# To have the same locations as the buildpack-based application, so
# Celery can always find the code for the DAGs
RUN mkdir -p /home/vcap/app
WORKDIR /home/vcap/app

COPY requirements.txt .
COPY requirements-dev.txt requirements-dev.txt

RUN pip install -r requirements-dev.txt

COPY . .