# call from repo root

FROM ubuntu:16.04 AS build

ENV GOLANG_VERSION 1.15
ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH
ENV CGO_ENABLED=0

WORKDIR /go/src/gpushare

COPY  . .
RUN apt update && \
    apt install -y g++ wget make && \
    wget -nv -O - https://storage.googleapis.com/golang/go${GOLANG_VERSION}.linux-amd64.tar.gz | tar -C /usr/local -xz

RUN cd cmd/gpu-vpod-manager && \
    go build -o gpushare-vpod-manager main.go

FROM alpine:3.9

COPY --from=build /go/src/gpushare/cmd/gpu-vpod-manager/gpushare-vpod-manager /usr/bin/gpushare-vpod-manager

CMD ["gpushare-vpod-manager", "-alsologtostderr", "-v=4"]
