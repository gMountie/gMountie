FROM alpine:latest AS server

ARG TARGETPLATFORM

COPY $TARGETPLATFORM/gMountie /opt/gmountie/gMountie
ENTRYPOINT ["/opt/gmountie/gMountie", "serve"]
