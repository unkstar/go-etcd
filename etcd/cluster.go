package etcd

import (
	"net/url"
	"sync/atomic"
	"unsafe"
)

type Cluster struct {
	Leader   string   `json:"leader"`
	Machines []string `json:"machines"`
}

func NewCluster(machines []string) *Cluster {
	// if an empty slice was sent in then just assume HTTP 4001 on localhost
	if len(machines) == 0 {
		machines = []string{"http://127.0.0.1:4001"}
	}

	// default leader and machines
	return &Cluster{
		Leader:   machines[0],
		Machines: machines,
	}
}

// switchLeader switch the current leader to machines[num % len(machines)]
func (a *AtomicCluster) switchLeader(num int) {
	old := a.get()
	clone := old.Clone()

	newLeader := num % len(clone.Machines)

	logger.Debugf("switch.leader[from %v to %v]",
		clone.Leader, clone.Machines[newLeader])

	clone.Leader = clone.Machines[newLeader]

	if !a.cas(old, clone) {
		logger.Debugf("switch.leader to %v interrupted, give up",
			clone.Leader)
	}
}

func (a *AtomicCluster) updateLeader(leader string) {
	old := a.get()
	clone := old.Clone()

	logger.Debugf("update.leader[from %v to %v]",
		clone.Leader, leader)

	clone.Leader = leader

	if !a.cas(old, clone) {
		logger.Debugf("update.leader to %v interrupted, give up",
			clone.Leader)
	}
}

func (a *AtomicCluster) updateLeaderFromURL(u *url.URL) {
	var leader string
	if u.Scheme == "" {
		leader = "http://" + u.Host
	} else {
		leader = u.Scheme + "://" + u.Host
	}
	a.updateLeader(leader)
}

func (cl *Cluster) Clone() *Cluster {
	return &Cluster{
		Leader:   cl.Leader,
		Machines: cl.Machines,
	}
}

type AtomicCluster struct{ p unsafe.Pointer }

func (a *AtomicCluster) get() *Cluster {
	ptr := atomic.LoadPointer(&a.p)
	return (*Cluster)(ptr)
}

func (a *AtomicCluster) set(c *Cluster) {
	ptr := unsafe.Pointer(c)
	atomic.StorePointer(&a.p, ptr)
}

func (a *AtomicCluster) cas(oldValue *Cluster, newValue *Cluster) bool {
	oldPtr := unsafe.Pointer(oldValue)
	newPtr := unsafe.Pointer(newValue)
	return atomic.CompareAndSwapPointer(&a.p, oldPtr, newPtr)
}
