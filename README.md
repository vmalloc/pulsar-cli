# pulsar-cli

Small utility for producing and consuming topics using Apache Pulsar, written in Rust.

# Installtion

```
$ cargo install pulsar-cli
```

# Usage

```
# produce messages
$ pulsar-cli produce --topic <topic>
# consume messages
$ pulsar-cli consume --topic <topic> [--json]
```