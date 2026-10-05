#!/usr/bin/env bash
# Drops the benchmark's databases. hotelReservation seeds its databases on
# every service start without checking what is there, so on persistent
# volumes each restart duplicates the seed data (the rate inventory grows by
# 27 rate plans per restart) and the per-request cost drifts. Every run
# starts from empty databases instead.
for m in geo profile rate recommendation reservation user; do
  docker exec lchotel-mongodb-$m-1 mongo --quiet --eval '
    db.adminCommand({listDatabases: 1}).databases
      .filter(d => !["admin", "config", "local"].includes(d.name))
      .forEach(d => { db.getSiblingDB(d.name).dropDatabase(); print("dropped " + d.name); })' &
done
wait
