FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/notifications ./cmd/notifications

FROM alpine:3.19
WORKDIR /app
COPY --from=build /out/notifications /app/notifications
COPY migrations /app/migrations
ENTRYPOINT ["/app/notifications"]
