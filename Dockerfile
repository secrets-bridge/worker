# Build stage.
FROM golang:1.27.3-alpine@sha256:d200d2dd76b03a99b52651fd1325aff7dcd35422c35c01bac56571b0507f3deb AS build
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
FROM gcr.io/distroless/static:nonroot@sha256:d62067879de890a1def9da0e7766b7d20810a5e5555cef8506b752437d7a3e88
WORKDIR /
COPY --from=build /out/worker /worker
EXPOSE 8091
USER nonroot
ENTRYPOINT ["/worker"]
