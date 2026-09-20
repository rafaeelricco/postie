FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN go build -o /out/postie ./cmd/postie && go build -o /out/postiectl ./cmd/postiectl

FROM alpine:3.20
WORKDIR /app
COPY --from=build /out/postie /usr/local/bin/postie
COPY --from=build /out/postiectl /usr/local/bin/postiectl
COPY examples ./examples
ENTRYPOINT ["postie"]
