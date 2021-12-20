# call from repo root

FROM nvidia/cuda:10.0-devel AS build

WORKDIR /tmp/build

COPY ./libgemhook.so.1 /tmp/build/

# FROM busybox:1.31.1-glibc
FROM alpine:3.9

COPY --from=build /tmp/build/libgemhook.so.1 /libgemhook.so.1
#COPY --from=build /tmp/build/Gemini/lib/libgemhook.so.1 /libgemhook.so.1

CMD ["cp", "/libgemhook.so.1", "/gpushare/library/libgemhook.so.1"]

# 功能：将容器中的/libgemhook.so.1拷贝到各节点/gpushare/library/libgemhook.so.1中