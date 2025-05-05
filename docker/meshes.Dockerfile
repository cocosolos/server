FROM busybox:latest

COPY ./losmeshes /losmeshes
COPY ./navmeshes /navmeshes

VOLUME /navmeshes /losmeshes