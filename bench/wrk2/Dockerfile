# wrk2 as shipped in DeathStarBench, used to cross-check cmd/loadgen with the
# benchmark's own load generator and Lua script (mixed-workload_type_1.lua).
# Build context: the DeathStarBench checkout.
FROM ubuntu:24.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential libssl-dev zlib1g-dev luarocks lua5.1 liblua5.1-0-dev ca-certificates git unzip \
 && luarocks install luasocket && rm -rf /var/lib/apt/lists/*
COPY wrk2 /wrk2
RUN cd /wrk2 && make -j2 && cp wrk /usr/local/bin/wrk2
COPY hotelReservation/wrk2/scripts /scripts
ENTRYPOINT ["wrk2"]
