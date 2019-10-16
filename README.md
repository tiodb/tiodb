A new approach to distributed systems, using remote containers (vector, lists and maps) and publish/subscribe model.

  * Containers can hold strings, doubles and ints.
  * Data is written in a key/value/metadata model. Every container has a property map, that can be used to store the value schema (or something else). You can use metadata to store the schema per record (or something else)
  * Full publish/subscribe support for every change on every container. "Snapshot plus updates" model support.
  * Metacontainers with container list, user list, statistics, etc
  * Authorization per command per container (ACL), a user can be authorized to get items from a list but not to modify it.
  * Persistent and volatile containers. Persistence is done using a transaction log, and transaction records are **guaranteed** to be written when client receives the server response.
  * Two protocol: text based and binary. Using the text protocol you can test Tio using a telnet connection. The binary protocol is faster (~20%)
  * Support for distributing workload between clients with the WaitAndPop support
  * Support thousands of containers, only limited by RAM
  * Support for Python, C and C++ plugins
  * C API is pretty simple, so it can be easily ported to another platforms. It works on Windows, Linux, MacOSX and iOS
  * C++ API mimics STL, the Python API mimic the Python native containers
  * Still not as fast as it could be (10k inserts per seconds using volatile containers and 1.5k inserts per second using persistent containers with full fsync). No optimization effort was made yet, but the code is simple, pretty hackable and with plenty room for optimizations.

Tio can be used in many ways:

  * As Message Broker
  * As a [Queue Server](TioAsQueueServer.md) (like MSMQ or WebsphereMQ aka MQSeries)
  * As an Enterprise Service Bus ([Wikipedia](http://en.wikipedia.org/wiki/Enterprise_service_bus)), using http://code.google.com/p/primo as a process coordinator
  * As a [cluster coordinator](TioAsClusterCoordinator.md)
  * As [key/value store](TioAsKeyValueStore.md), like Redis, memcached, etc.

More details on StartHere and [Protocol](Protocol.md).

If you have any problem using Tio, please ask your question on the [Tio dicussion group](https://groups.google.com/forum/#!forum/tio-project)
