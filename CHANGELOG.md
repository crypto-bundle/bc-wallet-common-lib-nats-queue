# Change Log

## [v0.1.9] - 28.04.2023 12:03 MSK

### Changed

#### Switching to a proprietary license.
License of **bc-wallet-common-lib-nats-queue** repository changed to proprietary - commit revision number **5b3d003be0dbb5cd0a4aa1ac942ac84827ba4595**.

Origin repository - https://github.com/crypto-bundle/bc-wallet-common-lib-nats-queue

The MIT license is replaced by me (_Kotelnikov Aleksei_) as an author and maintainer.

The license has been replaced with a proprietary one, with the condition of maintaining the authorship
and specifying in the README.md file in the section of authors and contributors.

[@gudron (Kotelnikov Aleksei)](https://github.com/gudron) - author and maintainer of [crypto-bundle project](https://github.com/crypto-bundle)

The commit is signed with the key -
gudron2s@gmail.com
E456BB23A18A9347E952DBC6655133DD561BF3EC

## [v0.1.10] - 09.05.2023
### Added
* Nats helm-chart for local development. Chart cloned from [official Nats repository](https://github.com/nats-io/k8s/tree/main/helm/charts/nats)

## [v0.1.11] - 09.05.2023
### Changed
* Helm-chart changes:
  * Metrics exporter - set enabled state to false
  * NatsBox containers - set enabled state to false

## [v0.1.12] - 13.06.2023
### Changed
* Nats-config - added secret go-tag to nats user and password fields