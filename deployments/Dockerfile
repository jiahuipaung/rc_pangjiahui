FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/notifier ./cmd/notifier

FROM postgres:17-alpine AS migration
COPY migrations /migrations

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && addgroup -S notifier && adduser -S -G notifier notifier
COPY --from=build /out/notifier /usr/local/bin/notifier
COPY configs/destinations.example.yaml /etc/notifier/destinations.yaml
USER notifier:notifier
ENTRYPOINT ["/usr/local/bin/notifier"]
