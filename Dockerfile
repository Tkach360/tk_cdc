FROM golang:1.25.0-alpine AS builder
WORKDIR /tk_cdc

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/service ./cmd/server/main.go

FROM alpine:3.19
WORKDIR /tk_cdc
RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/service /app/service

# нужно указать флаг -config при запуске сервиса
ENTRYPOINT ["/tk_cdc/service"]
CMD ["-config", "/tk_cdc/configs/config.yml"]
