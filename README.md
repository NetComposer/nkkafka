
# NkKAFKA

Kafka package for NkSERVER services

This package providers a high performance connector to send and receive Kafka messages. It is capable of filling a 1Gbps connection reading or writing messages.

You can send and read individual messages with the utilities in [nkkafka](src/nkkafka.erl), but it is much faster to define _subscribers_ to read messages at scale. If the service is installed in an Erlang cluster, partitions will be splitted among them. Each topic and partition will open an independent connection to Kafka. See [nkkafka_plugin](src/nkkafka_plugin.erl) for available options. It is not uncommon to be able to read more than 100.000 msgs/seg (with several hundreds of bytes each) in 4-core machines.

To send messages, use a [producer](src/nkkafka_producers.erl). Producers are started and stopped automatically.





 

