FROM python:3.12.7-bookworm

RUN pip install poetry

WORKDIR /app

COPY . ./

RUN poetry install --no-root

ENTRYPOINT ["poetry", "run", "python", "stream_tweets_to_kafka.py"]