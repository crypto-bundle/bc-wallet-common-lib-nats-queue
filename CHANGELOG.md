# Change Log

## [v0.0.1] - 06.02.2023 21:42 MSK
### Added
* Connection config
* Connection wrapper with option management
* Consumer flow wrapper
  * Consumer worker pool for subscription via go-chanel
  * Consumer worker pool for subscription via pulling
  * Consumer worker
  * Added simple-queue consumer worker pool as independent service-component
* Producer flow
  * Producer worker pool
  * Producer worker
  * Added simple-queue producer worker pool as independent service-component
* Added common-queue decision directives
* Create consumer subscription as independent service component
  * JetStream push-based subscription via go-chanel
  * JetStream push-based queue-group subscription via go-chanel
  * Simple-queue push-based subscription
### Changed
* Removed check stream info for pull-type consumer worker-pool
* Removed creation of stream in producer creation flow
* Moved lib-nats-queue to another repository - https://github.com/crypto-bundle/bc-wallet-common-lib-nats-queue
### Fixed
* Fixed nats-config default values

## [v0.1.0 - v0.1.7~refactoring] - 19.04.2023 - 22.04.2023
### Added
* Added onConnect/onReconnect handlers
### Changed
* Refactoring of nats-queue consumer and producer flow
  * Added producer and consumer management via connection wrapper
  * Added more options for worker pools and subscription flow
    * JetStream single worker queue-group push type consumer
    * JetStream pull-type consumer via go-chanel
    * JetStream queue-group push type consumer via go-chanel

## [v0.1.8] - 23.04.2023
### Changed
* Small changes in connection wrapper service component
  * Added consumer and producer counters
* Added AckWait option for consumer worker

## [v0.1.9] - 09.02.2024 23:27 MSK
### Added
* Nats helm-chart for local development. Chart cloned from [official Nats repository](https://github.com/nats-io/k8s/tree/main/helm/charts/nats)
* Helm-chart changes:
  * Metrics exporter - set enabled state to false
  * NatsBox containers - set enabled state to false
* Nats-config - added secret go-tag to nats user and password fields
* Added NATS consumer config structs for
  * QueueGroup consumers
  * Standard consumers
* Added ReQueue delay option for Nack message case
* Added passing SubOptions for all type of consumers
* Added single worker producer service-component
* Single worker pull-type consumer
  * Consumer service-component
  * Consumer config
  * Subscription service-component
* AckWait option for consumers
* Added Pull-type channel-based worker pool and subscription
### Fixes
* Nak delay timings
### Changed
* Remake shutdown flow for producer and consumer components - removed flow with calling Shutdown() function.
  Added usage of context.WithCancel() flow.
* Changed go-namespace.
* Changed licence in README.md and LICENSE files

## [v0.1.10] - 14.04.2024
### Added
* Added support of healthcheck flow, which required by [lib-healthcheck](https://github.com/crypto-bundle/bc-wallet-common-lib-healthcheck)