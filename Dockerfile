FROM golang:1.25.0-alpine AS builder
WORKDIR /tk_cdc

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/service ./cmd/server/main.go

FROM alpine:3.19
RUN apk add --no-cache ca-certificates

COPY --from=builder /app/service /app/service

WORKDIR /app

ENTRYPOINT ["/app/service"]
CMD ["-config", "/app/configs/config.yml", "-port", "8082"]
