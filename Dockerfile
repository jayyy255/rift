# syntax=docker/dockerfile:1
FROM golang:1.26.11-alpine3.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X github.com/jayyy255/rift/internal/version.Version=${VERSION}" -o /out/rift-broker ./cmd/rift-broker \
 && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X github.com/jayyy255/rift/internal/version.Version=${VERSION}" -o /out/rift ./cmd/rift

FROM alpine:3.24
RUN adduser -D -u 10001 rift && mkdir -p /var/lib/rift && chown rift:rift /var/lib/rift
COPY --from=build /out/ /usr/local/bin/
USER rift
VOLUME /var/lib/rift
EXPOSE 7070 9100
ENTRYPOINT ["rift-broker"]
CMD ["--data-dir", "/var/lib/rift", "--listen", "0.0.0.0:7070", "--metrics-listen", "0.0.0.0:9100"]
