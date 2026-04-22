# Build stage
FROM golang:1.24.0-alpine3.21 AS builder

WORKDIR /app
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o /id-generator

# Final stage — just the binary
FROM alpine:3.21

COPY --from=builder /id-generator /id-generator

EXPOSE 8000

CMD ["/id-generator"]