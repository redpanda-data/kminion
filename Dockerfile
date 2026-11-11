############################################################
# Build image
############################################################
FROM golang:1.26.11-alpine AS builder

ARG VERSION
ARG BUILT_AT
ARG COMMIT

RUN apk update && apk upgrade --no-cache && apk add --no-cache git ca-certificates && update-ca-certificates

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build \
    -ldflags="-w -s \
    -X main.version=$VERSION \
    -X main.commit=$COMMIT \
    -X main.builtAt=$BUILT_AT" \
    -o ./bin/kminion

############################################################
# Runtime Image
############################################################
FROM alpine:3
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/bin/kminion /app/kminion
# Keep the UID and GID that adduser picked before, so existing setups do not change.
RUN addgroup -S -g 101 redpanda \
    && adduser -S -u 100 redpanda -G redpanda \
    && chmod o+rx /app/kminion \
    && apk upgrade --no-cache
# A numeric user lets Kubernetes check runAsNonRoot without a runAsUser.
USER 100:101

ENTRYPOINT ["/app/kminion"]
