# tc (iproute2) for netem fault injection. Run with --net container:<target>
# --cap-add NET_ADMIN to add delay to everything the target sends.
FROM alpine:3.20
RUN apk add --no-cache iproute2
ENTRYPOINT ["tc"]
