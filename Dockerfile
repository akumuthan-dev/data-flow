FROM python:3.7

RUN mkdir -p /app

ADD . /app

ENV PYTHONUNBUFFERED 1

WORKDIR /app

RUN pip install -r requirements.txt
RUN chmod +x ./entrypoint.sh

CMD ./entrypoint.sh
