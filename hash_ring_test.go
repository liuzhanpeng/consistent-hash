package consistent

import (
	"sort"
	"testing"
)

func TestNew(t *testing.T) {
	r := NewHashRing()
	if r == nil {
		t.Error("创建失败")
	}
}

func TestAdd(t *testing.T) {
	k := "192.168.0.1"
	c := 10

	r := NewHashRing()
	r.Add(k, c)

	if len(r.nodes) != c {
		t.Errorf("期待节点数:%d, 实际数: %d", c, len(r.nodes))
	}

	if len(r.sortedHashKeys) != c {
		t.Errorf("期待节点key数:%d, 实际数: %d", c, len(r.sortedHashKeys))
	}

	if !sort.IsSorted(r.sortedHashKeys) {
		t.Errorf("节点key列表没有排序")
	}

	_, ok := r.nodeReplicas[k]
	if !ok {
		t.Errorf("找不到节点虚拟数映射值")
	}
	if r.nodeReplicas[k] != c {
		t.Errorf("虚拟节点数期待值：%d, 实际值:%d", c, r.nodeReplicas[k])
	}

}

func TestAddMulti(t *testing.T) {
	m := map[string]int{
		"192.168.0.1": 10,
		"192.168.0.2": 10,
		"192.168.0.3": 5,
		"192.168.0.4": 5,
	}
	r := NewHashRing()
	for k, v := range m {
		r.Add(k, v)
	}

	if len(r.nodes) != 30 {
		t.Errorf("期待节点数:%d, 实际数: %d", 30, len(r.nodes))
	}

	if len(r.sortedHashKeys) != 30 {
		t.Errorf("期待节点key数:%d, 实际数: %d", 30, len(r.sortedHashKeys))
	}

	if !sort.IsSorted(r.sortedHashKeys) {
		t.Errorf("节点key列表没有排序")
	}

	_, ok := r.nodeReplicas["192.168.0.3"]
	if !ok {
		t.Errorf("找不到节点虚拟数映射值")
	}
	if r.nodeReplicas["192.168.0.3"] != 5 {
		t.Errorf("虚拟节点数期待值：%d, 实际值:%d", 5, r.nodeReplicas["192.168.0.3"])
	}
}

func TestRemove(t *testing.T) {
	r := NewHashRing()
	r.Add("192.168.0.1", 5)
	r.Add("192.168.0.2", 5)
	r.Add("192.168.0.3", 5)

	r.Remove("192.168.0.2")

	if len(r.nodes) != 10 {
		t.Errorf("期待节点数:%d, 实际数: %d", 10, len(r.nodes))
	}

	if len(r.sortedHashKeys) != 10 {
		t.Errorf("期待节点key数:%d, 实际数: %d", 10, len(r.sortedHashKeys))
	}

	if _, ok := r.nodeReplicas["192.168.0.2"]; ok {
		t.Errorf("删除节点失败")
	}

	if _, ok := r.nodeReplicas["192.168.0.1"]; !ok {
		t.Errorf("删除节点失败")
	}
}

func TestGet(t *testing.T) {
	m := map[string]int{
		"192.168.0.1": 100,
		"192.168.0.2": 100,
		"192.168.0.3": 100,
	}
	r := NewHashRing()
	for k, v := range m {
		r.Add(k, v)
	}

	data := []string{
		"abc", "qod", "198s", "w1cn", "1kf", "gaw", "e1f", "gao", "blq", "9sdk", "1ja",
	}

	for _, v := range data {
		k, err := r.Get(v)
		if err != nil {
			t.Errorf("Get err: %s", err)
		}
		if _, ok := m[k]; !ok {
			t.Errorf("超出预期值:%s", k)
		}
	}
}

func TestGetSingleNode(t *testing.T) {
	r := NewHashRing()
	r.Add("192.168.0.1", 10)

	l := []string{
		"aaaa",
		"bbb",
		"cccc",
	}

	for _, v := range l {
		k, err := r.Get(v)
		if err != nil {
			t.Errorf("Get err: %s", err)
		}
		if k != "192.168.0.1" {
			t.Errorf("超出预期值:%s", k)
		}
	}
}

func TestGetFromRomvedNode(t *testing.T) {
	m := map[string]int{
		"192.168.0.1": 100,
		"192.168.0.2": 100,
		"192.168.0.3": 100,
	}
	r := NewHashRing()
	for k, v := range m {
		r.Add(k, v)
	}

	data := []string{
		"abc", "qod", "198s", "w1cn", "1kf", "gaw", "e1f", "gao", "blq", "9sdk", "1ja",
	}
	for _, v := range data {
		k, _ := r.Get(v)
		t.Log(k)
	}
	r.Remove("192.168.0.2")
	t.Log("----")
	for _, v := range data {
		k, _ := r.Get(v)
		t.Log(k)
		if k == "192.168.0.2" {
			t.Errorf("超出预期值:%s", k)
		}
	}
}
