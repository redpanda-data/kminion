# build image
FROM golang:1.15-alpine as builder
RUN apk update && apk add --no-cache git ca-certificates && update-ca-certificates

WORKDIR /app
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -a -installsuffix cgo -o /go/bin/kafka-minion

# executable image
FROM alpine:3
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /go/bin/kafka-minion /go/bin/kafka-minion

ENV VERSION 1.0.2
ENTRYPOINT ["/go/bin/kafka-minion"]
