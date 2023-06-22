FROM golang:1.20-bullseye
LABEL authors="andy"

ARG GOOS=linux
WORKDIR /app
ADD . .
RUN CGO_ENABLED=0 GOOS=$GOOS go build -trimpath -ldflags "-s -w" -o snowflake-data-loader .
ENTRYPOINT ["/app/snowflake-data-loader"]
