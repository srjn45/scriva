FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata

COPY scriva /usr/local/bin/scriva

RUN adduser -D -H -h /data scriva && \
    mkdir -p /data && \
    chown scriva:scriva /data

USER scriva
WORKDIR /data

EXPOSE 5433 8080

# Inside a container the listeners must bind all interfaces; the server refuses
# to start this way unless an API key or TLS is configured (SCRIVA_API_KEY).
ENTRYPOINT ["scriva", "serve", "--data", "/data", "--grpc-addr", "0.0.0.0:5433", "--rest-addr", "0.0.0.0:8080"]
