FROM golang:1.21-alpine3.19 AS builder

WORKDIR /

COPY . .

RUN apk add --update build-base && apk upgrade --available
RUN go build -o /gravity-adapter-mongodb ./cmd/gravity-adapter-mongodb/gravity-adapter-mongodb.go

RUN cd /build/docker/pwd_encrypt && \
        go build -o /pwd_encrypt

FROM alpine:3.19.2
WORKDIR /

RUN apk update && apk upgrade --available && apk add tzdata

COPY --from=builder /gravity-adapter-mongodb /gravity-adapter-mongodb
COPY --from=builder /pwd_encrypt /pwd_encrypt
COPY ./configs /configs
COPY ./settings/ /settings/
COPY ./build/docker/startup.sh /startup.sh

RUN mkdir /statestore && \
        chown -R 1001:0  /settings /configs /statestore /gravity-adapter-mongodb /pwd_encrypt /startup.sh && \
        chmod 777 /settings/sources.json /configs/config.toml  && \
        chmod -R g+rwX /statestore

USER 1001
ENV TZ="Asia/Taipei"

CMD ["sh", "/startup.sh"]
