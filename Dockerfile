# Builds FireMQ from source into a tiny static image.
#   docker build -t firemq .
#   docker run -p 5672:5672 -p 5673:5673 -p 15672:15672 firemq
FROM rust:1.95-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p firemq \
 && mkdir -p /out/data \
 && cp target/release/firemq /out/firemq

FROM scratch
LABEL org.opencontainers.image.title="FireMQ" \
      org.opencontainers.image.description="A lean, fast, RabbitMQ-compatible message broker" \
      org.opencontainers.image.source="https://github.com/FireMQ/firemq" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /out/firemq /firemq
COPY --from=build /out/data /data
# In a container, guest connects through the container network, not loopback.
ENV FIREMQ_DATA_DIR=/data \
    FIREMQ_LOOPBACK_USERS="" \
    FIREMQ_HTTP_URL=http://127.0.0.1:15672
VOLUME /data
EXPOSE 5672 5671 5673 5674 15672
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s CMD ["/firemq", "health"]
ENTRYPOINT ["/firemq"]
