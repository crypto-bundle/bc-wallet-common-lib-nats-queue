# bc-wallet-common-lib-nats-queue

## Description

**bc-wallet-common-lib-nats-queue** its library for manage NATS entities like connections, producer and consumer service-components.
Also library support worker-pool conception for chanel-base subscriptions.


### Consumers 
Library contains wrapper service components for multiple consumer-subscription types:

- **JetStream** Go-chanel based **Push-type** consumer with queue-group support
- **JetStream** Go-chanel based **Pull-type** consumer with queue-group support
- **JetStream** Handler-func based **Push-type** consumer with queue-group support
- **Simple** Go-chanel based **Push-type** consumer with queue-group support

### Producers

## Contributors

* Author and maintainer - [@gudron (Alex V Kotelnikov)](https://github.com/gudron)

## Licence

**bc-wallet-common-lib-nats-queue** has a proprietary license.

Switched to proprietary license from MIT - [CHANGELOG.MD - v0.0.14](./CHANGELOG.md)