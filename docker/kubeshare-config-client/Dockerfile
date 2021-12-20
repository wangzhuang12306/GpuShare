# call from repo root

FROM ubuntu:16.04 AS build

ENV GOLANG_VERSION 1.15
ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

WORKDIR /go/src/gpushare

COPY  . .
RUN apt update && \
    apt install -y g++ wget make && \
    wget -nv -O - https://storage.googleapis.com/golang/go${GOLANG_VERSION}.linux-amd64.tar.gz | tar -C /usr/local -xz

RUN cd cmd/gpu-config-client && \
    go build -o gpushare-config-client main.go


FROM debian:stretch-slim

ENV NVIDIA_VISIBLE_DEVICES      all
ENV NVIDIA_DRIVER_CAPABILITIES  utility

COPY --from=build /go/src/gpushare/cmd/gpu-config-client/gpushare-config-client /usr/bin/gpushare-config-client

CMD ["gpushare-config-client", "-alsologtostderr", "-server-ip=gpushare-device-manager.kube-system.svc:9797", "-v=4"]
