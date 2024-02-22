FROM golang:1.21-alpine AS builder

WORKDIR /go/src/app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN go build -o ./vanflow-tool ./cmd

FROM alpine:latest

RUN adduser -D -u 10000 runner
USER 10000

WORKDIR /app
COPY --from=builder /go/src/app/vanflow-tool .
ENTRYPOINT ["/app/vanflow-tool"]
