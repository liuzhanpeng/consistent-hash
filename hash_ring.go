package consistent

import (
	"errors"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type hashKey uint32

// 节点key列表; 为实现排序和搜索功能，实现sort.Interface接口
type hashKeys []hashKey

func (keys hashKeys) Len() int {
	return len(keys)
}

func (keys hashKeys) Less(i, j int) bool {
	return keys[i] < keys[j]
}

func (keys hashKeys) Swap(i, j int) {
	keys[i], keys[j] = keys[j], keys[i]
}

var (
	ErrEmpty    = errors.New("没有任何节点")
	ErrNotFound = errors.New("没找到相关key")
)

type HashRing struct {
	nodes          map[hashKey]string // 节点列表
	sortedHashKeys hashKeys           // 排序的节点key列表
	nodeReplicas   map[string]int     // 节点与虚拟数映射
	sync.RWMutex
}

// 创建环
func NewHashRing() *HashRing {
	ring := new(HashRing)
	ring.nodes = make(map[hashKey]string)
	ring.sortedHashKeys = make(hashKeys, 20)
	ring.nodeReplicas = make(map[string]int)
	return ring
}

// 添加节点
func (ring *HashRing) Add(nodeKey string, replicas int) {
	if replicas <= 0 {
		replicas = 1
	}
	ring.Lock()
	defer ring.Unlock()
	for i := 0; i < replicas; i++ {
		hkey := ring.hash(nodeKey + "-" + strconv.Itoa(i))
		ring.nodes[hkey] = nodeKey
		ring.sortedHashKeys = append(ring.sortedHashKeys, hkey)
	}
	ring.nodeReplicas[nodeKey] = replicas
	ring.sortHashKeys()
}

// 删除节点
func (ring *HashRing) Remove(nodeKey string) error {
	ring.Lock()
	defer ring.Unlock()
	replicas, ok := ring.nodeReplicas[nodeKey]
	if !ok {
		return ErrNotFound
	}
	for i := 0; i < replicas; i++ {
		hkey := ring.hash(nodeKey + "-" + strconv.Itoa(i))
		delete(ring.nodes, hkey)
	}
	delete(ring.nodeReplicas, nodeKey)
	ring.sortHashKeys()
	return nil
}

// 获取指定name对应的节点key
func (ring *HashRing) Get(name string) (string, error) {
	if len(ring.nodes) == 0 {
		return "", ErrEmpty
	}
	ring.RLock()
	defer ring.RUnlock()
	hkey := ring.hash(name)
	index := ring.search(hkey)
	return ring.nodes[ring.sortedHashKeys[index]], nil
}

// 排序
func (ring *HashRing) sortHashKeys() {
	hashKeys := ring.sortedHashKeys[:0]
	for k := range ring.nodes {
		hashKeys = append(hashKeys, k)
	}
	sort.Sort(hashKeys)
	ring.sortedHashKeys = hashKeys
}

func (ring *HashRing) search(hkey hashKey) int {
	index := sort.Search(len(ring.sortedHashKeys), func(x int) bool {
		return ring.sortedHashKeys[x] > hkey
	})
	// 如果大于最大节点的hash值，就返回第一个节点索引
	if index >= len(ring.sortedHashKeys) {
		return 0
	}
	return index
}

func (ring *HashRing) hash(key string) hashKey {
	return hashKey(crc32.ChecksumIEEE([]byte(key)))
}
