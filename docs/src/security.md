# Security

## Users and passwords

On first start, FireMQ creates one user, `guest` with password `guest` by default, with the `administrator` tag and full access to the `/` vhost. Like RabbitMQ, `guest` can only connect from localhost. Before exposing FireMQ to a network:

1. Create your own users and grant them access. These commands run as `guest`, from the broker's machine:

   ```bash
   firemq ctl add-user admin 'a-long-password' --tags administrator
   firemq ctl set-permissions admin
   firemq ctl add-user orders-service 'another-password'
   firemq ctl set-permissions orders-service --configure '^orders\.' --write '^orders\.' --read '^orders\.'
   ```

2. Delete `guest`, logged in as the new administrator:

   ```bash
   firemq ctl delete-user guest --user admin --pass 'a-long-password'
   ```

Alternatively, set `FIREMQ_DEFAULT_USER` and `FIREMQ_DEFAULT_PASS` before the first start, so `guest` is never created.

Passwords are stored as salted SHA-256 hashes in RabbitMQ's format, so users exported from RabbitMQ keep their passwords when imported.

## Permissions

Permissions work as in RabbitMQ. Each user has, per vhost, three regular expressions matched against queue and exchange names:

| Permission | Needed to |
|---|---|
| configure | declare and delete queues and exchanges |
| write | publish to an exchange, bind a queue or exchange as destination |
| read | consume from or get from a queue, purge it, bind an exchange as source |

An empty pattern grants nothing. Publishing to the default exchange checks write access to `amq.default`. A user without permissions on a vhost cannot connect to it.

## TLS

Pass a PEM certificate chain and private key:

```bash
firemq --tls-cert /etc/firemq/cert.pem --tls-key /etc/firemq/key.pem
```

This starts AMQPS on 5671 and the FireMQ protocol over TLS on 5674, next to the plain listeners. To accept only TLS, turn the plain ones off with `--amqp off --fire off`. FireMQ uses rustls and supports TLS 1.2 and 1.3.

The HTTP listener serves plain HTTP. To serve the UI, API and WebSocket over HTTPS, put a TLS-terminating proxy such as Caddy, nginx or a cloud load balancer in front of port 15672.

## Network exposure

Every listener binds to all interfaces by default. To keep a port local, bind it to the loopback address, for example `--http 127.0.0.1:15672`.
