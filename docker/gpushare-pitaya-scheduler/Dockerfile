# call from repo root 1

FROM nvidia/cuda:10.0-devel AS build

WORKDIR /tmp/build

COPY . /tmp/build



FROM python:3.8.1-buster

RUN pip3 install inotify

ENV NVIDIA_VISIBLE_DEVICES      all
ENV NVIDIA_DRIVER_CAPABILITIES  utility

COPY  --from=build /tmp/build/lib/libdl.so.2          /lib
COPY --from=build  /tmp/build/lib/libnvidia-ml.so.1   /lib
COPY  --from=build /tmp/build/launcher-multigpus.sh /launcher-multigpus.sh
COPY  --from=build /tmp/build/launcher.py /launcher.py
COPY --from=build /tmp/build/gem-schd /gem-schd
COPY --from=build /tmp/build/gem-pmgr /gem-pmgr

CMD ["/launcher-multigpus.sh", "/gpushare/scheduler/config", "/gpushare/scheduler/podmanagerport"]
