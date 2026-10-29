# syntax=docker/dockerfile:1
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# -mod=readonly: the build uses exactly what go.mod and go.sum pin, and fails
# rather than quietly resolving something new.
RUN CGO_ENABLED=0 go build -mod=readonly -trimpath -ldflags="-s -w" -o /out/app ./cmd/telemetry-processor

# Distroless: no shell, no package manager. You debug from metrics, logs and
# ephemeral debug containers, the way you would in production.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
