FROM postgres:latest

ENV POSTGRES_DB exchangedb
ENV PGUSER postgres
ENV POSTGRES_PASSWORD password

VOLUME exchange:/var/lib/postgressql/exchange

EXPOSE 5432