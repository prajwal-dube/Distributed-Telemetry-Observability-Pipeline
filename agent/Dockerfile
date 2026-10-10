FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /agent ./cmd/agent
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /agent /agent
EXPOSE 8083
ENTRYPOINT ["/agent"]
