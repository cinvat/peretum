FROM golang:latest AS builder

ENV HOME=/
ENV CGO_ENABLED=0
ENV GOOS=linux

WORKDIR /

COPY . .

RUN go build -trimpath -buildvcs=false -ldflags="-s -w" -o peretum .

FROM alpine:3.23.4
RUN apk add ca-certificates
WORKDIR /
COPY --from=builder /peretum .

ENTRYPOINT ["/peretum"]
