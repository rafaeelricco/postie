FROM --platform=$BUILDPLATFORM golang:1.27.1-alpine AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/postie ./cmd/postie \
 && CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/postiectl ./cmd/postiectl

FROM alpine:3.20
WORKDIR /app
COPY --from=build /out/postie /usr/local/bin/postie
COPY --from=build /out/postiectl /usr/local/bin/postiectl
COPY examples ./examples
EXPOSE 8081
USER 65532:65532
ENTRYPOINT ["postie"]
