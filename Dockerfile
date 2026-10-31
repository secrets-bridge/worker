# Build stage.
FROM golang:1.27.2-alpine@sha256:f92b6ef800e499660581efdabdf25d9d817a9d124eaf900924f0504e7e27e12d AS build
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
FROM gcr.io/distroless/static:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99
WORKDIR /
COPY --from=build /out/worker /worker
EXPOSE 8091
USER nonroot
ENTRYPOINT ["/worker"]
