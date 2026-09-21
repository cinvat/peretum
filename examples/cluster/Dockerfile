# Build the peretum binary
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git make

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o peretum .

# Runtime image
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata wget

# Create non-root user
RUN addgroup -g 1000 -S peretum && \
    adduser -u 1000 -S peretum -G peretum

# Mode argument: edge (default) or controlplane
ARG MODE=edge

WORKDIR /app

COPY --from=builder /app/peretum /app/peretum

# Copy config files
COPY config.yaml /app/config.yaml
COPY config.d/ /app/config.d/

RUN mkdir -p /cache /data && chown -R peretum:peretum /app /cache /data

USER peretum

# Expose ports based on mode
EXPOSE 8080 8443 9090 9001

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
  CMD wget -q --spider http://localhost:8080/health || exit 1

# Default to edge mode, can be overridden with --entrypoint or env
ENTRYPOINT ["/app/peretum"]
CMD [""]