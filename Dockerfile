# syntax=docker/dockerfile:1.7
FROM golang:1.25.13-alpine3.24 AS build

ARG SERVICE
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN test -n "$SERVICE" && CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -ldflags="-s -w" -o /out/incidentflow "./cmd/${SERVICE}"

FROM alpine:3.24
RUN apk add --no-cache ca-certificates && adduser -D -H -u 10001 incidentflow
USER incidentflow
COPY --from=build /out/incidentflow /usr/local/bin/incidentflow
ENTRYPOINT ["/usr/local/bin/incidentflow"]
