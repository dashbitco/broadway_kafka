# Changelog

## v0.4.4 (2024-07-08)

 * Update brod dependency to ~> 3.6 or ~> 4.0

## v0.4.3 (2024-06-12)

  * Support setting socket options on Kafka connection

## v0.4.2 (2024-04-01)

  * Add support for timestamp `:offset_reset_policy`
  * Supports reading sasl credentials from file
  * Add `:begin_offset` option
  * Do not block until coordinator exits to avoid deadlocks

## v0.4.1 (2023-03-14)

  * Disable `offset_commit_on_ack` during revoke assignment call

## v0.4.0 (2022-08-23)

  * Implement fairer distribution of messages across partitions
  * Add :request_timeout option to BrodClient
  * Send telemetry when assignments_revoked is executed

## v0.3.6 (2022-06-22)

  * Do not poll while draining
  * Properly shutdown Brod's group coordinators
  * Support `:rebalance_timeout_seconds` option
  * Support `:query_api_versions` option

## v0.3.5 (2022-05-12)

  * Do not send more messages per partition than `max_demand`
  * Support for custom `:sasl` authenticators by allowing :callback to be sent as opts

## v0.3.4 (2022-03-19)

  * Pass SSL configurations when validating offset

## v0.3.3 (2022-02-16)

  * Ensure `handle_info` does not crash when receiving an EXIT message in case Kafka goes offline

## v0.3.2 (2022-02-15)

  * Ensure reset_policy is only used when offset is `undefined` or `out_of_range`

## v0.3.1 (2022-01-19)

  * Allow `:sasl` option to be set to `:undefined`
  * Allow `:heartbeat_rate_seconds` option to group config
  * Handle `:offset_out_of_range` errors when resolving offset
  * Ensure `brod` processes terminate on disconnections

## v0.3.0 (2021-08-30)

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
