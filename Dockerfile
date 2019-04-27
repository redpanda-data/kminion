# build image
FROM golang:1.11-alpine as builder
RUN apk update && apk add --no-cache git ca-certificates && update-ca-certificates

WORKDIR /app
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -a -installsuffix cgo -o /go/bin/kafka-lag-exporter

# executable image
FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /go/bin/kafka-lag-exporter /go/bin/kafka-lag-exporter

ENV VERSION 0.1.2
ENTRYPOINT ["/go/bin/kafka-lag-exporter"]