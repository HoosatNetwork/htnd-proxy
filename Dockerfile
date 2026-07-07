FROM golang:1.26 AS build

ENV GOEXPERIMENT=simd,jsonv2

WORKDIR /src

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates git && \
    rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /out/htnd-proxy .

FROM ubuntu:24.04
WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /out/htnd-proxy /app/htnd-proxy

RUN chown nobody:nogroup /app/htnd-proxy && chmod +x /app/htnd-proxy

ENV listen=0.0.0.0:42420
ENV LOCAL_PEER_HOST=127.0.0.1
ENV LOCAL_PEER_PORT=42520
ENV MANUAL_RPC_PEERS=188.241.30.193

USER nobody
EXPOSE 42420

ENTRYPOINT ["/app/htnd-proxy"]
CMD ["-refresh=60"]