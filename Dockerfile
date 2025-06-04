FROM golang:1.23-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o sales-app

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/sales-app .
EXPOSE 8080
CMD ["/app/sales-app"]