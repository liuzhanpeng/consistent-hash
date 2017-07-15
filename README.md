# consistent-hash

根据原理实现一次一致性hash算法, 加深了解

## simple

```go

r := NewHashRing()
r.Add("192.168.0.1", 100)
r.Add("192.168.0.2", 100)

k, err := r.Get("test")
if err != nil {
    log.Fatal(err)
}

fmt.Println(k) // output 192.168.0.1

```