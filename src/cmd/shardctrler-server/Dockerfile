# Dockerfile for shardctrler server
FROM golang:1.20-alpine AS builder

WORKDIR /app/shardctrler

COPY go.mod go.sum ../
RUN go mod download

COPY . .
RUN go build -o server main_server.go

FROM alpine:latest

WORKDIR /root/server
COPY --from=builder /app/shardctrler/server .

EXPOSE 8080
CMD ["./server"]
