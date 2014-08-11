hashring
============================

Implements consistent hashing that can be used when
the number of server nodes can increase or decrease (like in memcached).
The hashing ring is built using the same algorithm as libketama.

This is a port of Python hash_ring library <https://pypi.python.org/pypi/hash_ring/>
in Go with the added methods of adding and removing nodes.


Using
============================

```go
import "github.com/serialx/hashring"
```

```go
memcacheServers := []string{'192.168.0.246:11212',
                            '192.168.0.247:11212',
                            '192.168.0.249:11212'}
ring := hashring.HashRing(memcacheServers)
server := ring.GetNode("my_key")
```
