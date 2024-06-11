# Dockerfile.client

# Builder stage
FROM golang:1.20-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum
COPY ../../../go.mod ../../../go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the Go application
RUN go build -o shardctrler-client

# Final stage
FROM alpine:latest

WORKDIR /root/shardctrler

# Copy the binary from the builder stage
COPY --from=builder /app/shardctrler-client .

# Expose the port if necessary
EXPOSE 8080

# Command to run the executable
CMD ["./shardctrler-client"]
