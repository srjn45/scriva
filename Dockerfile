FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata

COPY scriva /usr/local/bin/scriva

RUN adduser -D -H -h /data scriva && \
    mkdir -p /data && \
    chown scriva:scriva /data

USER scriva
WORKDIR /data

EXPOSE 5433 8080

ENTRYPOINT ["scriva", "serve", "--data", "/data"]
