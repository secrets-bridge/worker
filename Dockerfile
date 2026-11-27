# Build stage.
FROM golang:1.27.4-alpine@sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280 AS build
WORKDIR /src

# Copy module manifests first so the layer caches when only source
# changes. The replace directive pointing at ../api means CI builds
# need both repos in scope; this Dockerfile is for the published-deps
# case (CI replaces the replace before building).
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w -X main.buildVersion=$(git rev-parse --short HEAD 2>/dev/null || echo dev)" \
    -o /out/worker ./cmd/worker

# Runtime stage.
FROM gcr.io/distroless/static:nonroot@sha256:8ce6dd87e9823c5b48dcfb2c8604714ccb351ce59607327bcc4957448da279b5
WORKDIR /
COPY --from=build /out/worker /worker
EXPOSE 8091
USER nonroot
ENTRYPOINT ["/worker"]
