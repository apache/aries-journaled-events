# Apache Aries Journaled events

[![Build Status](https://builds.apache.org/buildStatus/icon?job=Aries-journaled-events)](https://builds.apache.org/job/Aries-journaled-events/)

This subproject provides an API (possibly as spec) and backends for journaled streams of events. These extend the publish/subscribe model with means to start consume from an point in the stored event stream history.

## Goals

* Provide traditional publish / subscribe semantics
* Allow consuming a stream from any point in the history (given it is not yet evicted)

## Non goals (to be discussed)

* No coverage of extreme scaling like Apache Kafka. So no sharding support in the API (like partitions).

## Requirements

* Messages sent to a topic must be journaled and must be available to all consumers
* Consumers must be able to start consuming messages from any point in the history that is still available
* If a consumer specifies a position that is not available anymore then it must start with the oldest message
* Each consumer must receive messages in the same order they were sent
* The journal of each topic may evict messages that are older than a certain retention time

## Modules

