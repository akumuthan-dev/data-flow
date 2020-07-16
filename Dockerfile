FROM python:3.7
ENV PYTHONUNBUFFERED 1

# Install dockerize https://github.com/jwilder/dockerize
ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

RUN mkdir -p /app
WORKDIR /app

COPY requirements.txt .
COPY requirements-dev.txt requirements-dev.txt

RUN pip install -r requirements-dev.txt

COPY . .