# syntax=docker/dockerfile:1
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
# -mod=mod lets the build complete go.sum when it is missing or stale.
# Once go.sum is committed and kept tidy, switch to -mod=readonly.
RUN CGO_ENABLED=0 go build -mod=mod -trimpath -ldflags="-s -w" -o /out/app ./cmd/telemetry-processor

# Distroless: no shell, no package manager. You debug from metrics, logs and
# ephemeral debug containers, the way you would in production.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
