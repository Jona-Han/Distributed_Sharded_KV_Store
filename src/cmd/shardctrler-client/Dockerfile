# Dockerfile for shardctrler client
FROM golang:1.20-alpine AS builder

WORKDIR /app/shardctrler

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o client main_client.go

FROM alpine:latest

WORKDIR /root/shardctrler
COPY --from=builder /app/shardctrler/client .

EXPOSE 8081
CMD ["./client"]
