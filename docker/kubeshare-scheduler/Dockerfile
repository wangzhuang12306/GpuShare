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

RUN cd cmd/gpu-scheduler && \
    go build -o gpushare-scheduler main.go


FROM alpine:3.9
ENV CGO_ENABLED=0
COPY --from=build /go/src/gpushare/cmd/gpu-scheduler/gpushare-scheduler /usr/bin/gpushare-scheduler

CMD ["gpushare-scheduler", "-alsologtostderr", "-v=4"]


# docker cp . nifty_maxwell:/go

# export GOLANG_VERSION=1.13.5
# export GOPATH=/go
# export PATH=$GOPATH/bin:/usr/local/go/bin:$PATH
# mkdir -p /go/src/KubeShare
# cd /go/src/KubeShare
