############################################################
# Build image
############################################################
FROM --platform=$BUILDPLATFORM golang:1.17-alpine as builder
RUN apk update && apk add --no-cache git ca-certificates && update-ca-certificates

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

ARG TARGETOS TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 go build -o ./bin/kminion

############################################################
# Runtime Image
############################################################
FROM --platform=$TARGETPLATFORM alpine:3
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/bin/kminion /app/kminion

# Embed env vars in final image as well (so the backend can read them)
ARG KMINION_VERSION
ENV VERSION ${KMINION_VERSION}

ENTRYPOINT ["/app/kminion"]
