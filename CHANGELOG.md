# Changelog

## v0.3.0-dev

  * Support `:client_id_prefix` to make it easier to identify Kafka connections
  * Add support for `:max_wait_time` in fetch
  * Require Broadway 1.0

## v0.2.0 (2021-03-11)

  * Add an API for updating topics on producers
  * Support consuming compacted topics

## v0.1.4 (2020-07-25)

  * Relax Brod dependency

## v0.1.3 (2020-04-27)

  * Fix resetting offset on every assignment

## v0.1.2 (2020-04-02)

  * Add support for single string hosts configuration
  * Add support for tuple lists with string as key in hosts
  * Drop invalid support of topic/partition for topics option

## v0.1.1 (2020-02-28)

  * Add SASL authentication support
  * Allow boolean for client config ssl option
  * Append `headers` to message metadata

## v0.1.0 (2020-02-19)

  * Initial release
