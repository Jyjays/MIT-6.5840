package shardkv

import (
	"context"
	"net/http"
	"os"
	"testing"

	"6.5840/labrpc"
	"6.5840/shardctrler"

	// import "log"
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math/big"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"time"

	"6.5840/raft"
)

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func makeSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

// Randomize server handles
func random_handles(kvh []*labrpc.ClientEnd) []*labrpc.ClientEnd {
	sa := make([]*labrpc.ClientEnd, len(kvh))
	copy(sa, kvh)
	for i := range sa {
		j := rand.Intn(i + 1)
		sa[i], sa[j] = sa[j], sa[i]
	}
	return sa
}

type group struct {
	gid       int
	servers   []*ShardKV
	saved     []*raft.Persister
	endnames  [][]string
	mendnames [][]string
}

// a replicated shardctrler service.
type ctrler struct {
	n       int
	servers []*shardctrler.ShardCtrler
	names   []string
	ck      *shardctrler.Clerk
}

type config struct {
	mu    sync.Mutex
	t     *testing.T
	net   *labrpc.Network
	start time.Time // time at which make_config() was called

	ctl *ctrler // shardctrler service

	ngroups int
	n       int // servers per k/v group
	groups  []*group

	clerks       map[*Clerk][]string
	nextClientId int
	maxraftstate int

	// 监控服务相关
	monitorServer *http.Server
	monitorCtx    context.Context
	monitorCancel context.CancelFunc
	monitor       *Monitor
}

func (cfg *config) checkTimeout() {
	// enforce a two minute real-time limit on each test
	if !cfg.t.Failed() && time.Since(cfg.start) > 120*time.Second {
		cfg.t.Fatal("test took longer than 120 seconds")
	}
}

func (cfg *config) cleanup() {
	// 停止监控服务
	cfg.stopMonitor()

	for gi := 0; gi < cfg.ngroups; gi++ {
		cfg.ShutdownGroup(gi)
	}
	for i := 0; i < cfg.ctl.n; i++ {
		cfg.ctl.servers[i].Kill()
	}
	cfg.net.Cleanup()
	cfg.checkTimeout()
}

// check that no server's log is too big.
func (cfg *config) checklogs() {
	for gi := 0; gi < cfg.ngroups; gi++ {
		for i := 0; i < cfg.n; i++ {
			raft := cfg.groups[gi].saved[i].RaftStateSize()
			snap := len(cfg.groups[gi].saved[i].ReadSnapshot())
			if cfg.maxraftstate >= 0 && raft > 8*cfg.maxraftstate {
				cfg.t.Fatalf("persister.RaftStateSize() %v, but maxraftstate %v",
					raft, cfg.maxraftstate)
			}
			if cfg.maxraftstate < 0 && snap > 0 {
				cfg.t.Fatalf("maxraftstate is -1, but snapshot is non-empty!")
			}
		}
	}
}

// controller server name for labrpc.
func (ctl *ctrler) ctrlername(i int) string {
	return ctl.names[i]
}

// shard server name for labrpc.
// i'th server of group gid.
func (cfg *config) servername(gid int, i int) string {
	return "server-" + strconv.Itoa(gid) + "-" + strconv.Itoa(i)
}

func (cfg *config) makeClient(ctl *ctrler) *Clerk {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// ClientEnds to talk to controller service.
	ends := make([]*labrpc.ClientEnd, ctl.n)
	endnames := make([]string, cfg.n)
	for j := 0; j < ctl.n; j++ {
		endnames[j] = randstring(20)
		ends[j] = cfg.net.MakeEnd(endnames[j])
		cfg.net.Connect(endnames[j], ctl.ctrlername(j))
		cfg.net.Enable(endnames[j], true)
	}

	ck := MakeClerk(ends, func(servername string) *labrpc.ClientEnd {
		name := randstring(20)
		end := cfg.net.MakeEnd(name)
		cfg.net.Connect(name, servername)
		cfg.net.Enable(name, true)
		return end
	})
	cfg.clerks[ck] = endnames
	cfg.nextClientId++
	return ck
}

func (cfg *config) deleteClient(ck *Clerk) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	v := cfg.clerks[ck]
	for i := 0; i < len(v); i++ {
		os.Remove(v[i])
	}
	delete(cfg.clerks, ck)
}

// Shutdown i'th server of gi'th group, by isolating it
func (cfg *config) ShutdownServer(gi int, i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	gg := cfg.groups[gi]

	// prevent this server from sending
	for j := 0; j < len(gg.servers); j++ {
		name := gg.endnames[i][j]
		cfg.net.Enable(name, false)
	}
	for j := 0; j < len(gg.mendnames[i]); j++ {
		name := gg.mendnames[i][j]
		cfg.net.Enable(name, false)
	}

	// disable client connections to the server.
	// it's important to do this before creating
	// the new Persister in saved[i], to avoid
	// the possibility of the server returning a
	// positive reply to an Append but persisting
	// the result in the superseded Persister.
	cfg.net.DeleteServer(cfg.servername(gg.gid, i))

	// a fresh persister, in case old instance
	// continues to update the Persister.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if gg.saved[i] != nil {
		gg.saved[i] = gg.saved[i].Copy()
	}

	kv := gg.servers[i]
	if kv != nil {
		cfg.mu.Unlock()
		kv.Kill()
		cfg.mu.Lock()
		gg.servers[i] = nil
	}
}

func (cfg *config) ShutdownGroup(gi int) {
	for i := 0; i < cfg.n; i++ {
		cfg.ShutdownServer(gi, i)
	}
}

// start i'th server in gi'th group
func (cfg *config) StartServer(gi int, i int) {
	cfg.mu.Lock()

	gg := cfg.groups[gi]

	// a fresh set of outgoing ClientEnd names
	// to talk to other servers in this group.
	gg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		gg.endnames[i][j] = randstring(20)
	}

	// and the connections to other servers in this group.
	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(gg.endnames[i][j])
		cfg.net.Connect(gg.endnames[i][j], cfg.servername(gg.gid, j))
		cfg.net.Enable(gg.endnames[i][j], true)
	}

	// ends to talk to shardctrler service
	mends := make([]*labrpc.ClientEnd, cfg.ctl.n)
	gg.mendnames[i] = make([]string, cfg.ctl.n)
	for j := 0; j < cfg.ctl.n; j++ {
		gg.mendnames[i][j] = randstring(20)
		mends[j] = cfg.net.MakeEnd(gg.mendnames[i][j])
		cfg.net.Connect(gg.mendnames[i][j], cfg.ctl.ctrlername(j))
		cfg.net.Enable(gg.mendnames[i][j], true)
	}

	// a fresh persister, so old instance doesn't overwrite
	// new instance's persisted state.
	// give the fresh persister a copy of the old persister's
	// state, so that the spec is that we pass StartKVServer()
	// the last persisted state.
	if gg.saved[i] != nil {
		gg.saved[i] = gg.saved[i].Copy()
	} else {
		gg.saved[i] = raft.MakePersister()
	}
	cfg.mu.Unlock()

	gg.servers[i] = StartServer(ends, i, gg.saved[i], cfg.maxraftstate,
		gg.gid, mends,
		func(servername string) *labrpc.ClientEnd {
			name := randstring(20)
			end := cfg.net.MakeEnd(name)
			cfg.net.Connect(name, servername)
			cfg.net.Enable(name, true)
			return end
		})

	kvsvc := labrpc.MakeService(gg.servers[i])
	rfsvc := labrpc.MakeService(gg.servers[i].rf)
	srv := labrpc.MakeServer()
	srv.AddService(kvsvc)
	srv.AddService(rfsvc)
	cfg.net.AddServer(cfg.servername(gg.gid, i), srv)
}

func (cfg *config) StartGroup(gi int) {
	for i := 0; i < cfg.n; i++ {
		cfg.StartServer(gi, i)
	}
}

func (cfg *config) StartCtrlerServer(ctl *ctrler, i int) {
	// ClientEnds to talk to other controller replicas.
	ends := make([]*labrpc.ClientEnd, ctl.n)
	for j := 0; j < ctl.n; j++ {
		endname := randstring(20)
		ends[j] = cfg.net.MakeEnd(endname)
		cfg.net.Connect(endname, ctl.ctrlername(j))
		cfg.net.Enable(endname, true)
	}

	p := raft.MakePersister()

	ctl.servers[i] = shardctrler.StartServer(ends, i, p)

	msvc := labrpc.MakeService(ctl.servers[i])
	rfsvc := labrpc.MakeService(ctl.servers[i].Raft())
	srv := labrpc.MakeServer()
	srv.AddService(msvc)
	srv.AddService(rfsvc)
	cfg.net.AddServer(ctl.ctrlername(i), srv)
}

func (cfg *config) ctrlerclerk(ctl *ctrler) *shardctrler.Clerk {
	// ClientEnds to talk to ctrler service.
	ends := make([]*labrpc.ClientEnd, ctl.n)
	for j := 0; j < ctl.n; j++ {
		name := randstring(20)
		ends[j] = cfg.net.MakeEnd(name)
		cfg.net.Connect(name, ctl.ctrlername(j))
		cfg.net.Enable(name, true)
	}

	return shardctrler.MakeClerk(ends)
}

// tell the shardctrler that a group is joining.
func (cfg *config) join(gi int) {
	cfg.joinm([]int{gi}, cfg.ctl)
}

func (cfg *config) ctljoin(gi int, ctl *ctrler) {
	cfg.joinm([]int{gi}, ctl)
}

func (cfg *config) joinm(gis []int, ctl *ctrler) {
	m := make(map[int][]string, len(gis))
	for _, g := range gis {
		gid := cfg.groups[g].gid
		servernames := make([]string, cfg.n)
		for i := 0; i < cfg.n; i++ {
			servernames[i] = cfg.servername(gid, i)
		}
		m[gid] = servernames
	}
	ctl.ck.Join(m)
}

// tell the shardctrler that a group is leaving.
func (cfg *config) leave(gi int) {
	cfg.leavem([]int{gi})
}

func (cfg *config) leavem(gis []int) {
	gids := make([]int, 0, len(gis))
	for _, g := range gis {
		gids = append(gids, cfg.groups[g].gid)
	}
	cfg.ctl.ck.Leave(gids)
}

func (cfg *config) StartCtrlerService() *ctrler {
	ctl := &ctrler{}
	ctl.n = 3
	ctl.servers = make([]*shardctrler.ShardCtrler, ctl.n)
	ctl.names = make([]string, ctl.n)
	for i := 0; i < ctl.n; i++ {
		ctl.names[i] = "ctlr-" + randstring(20)
	}
	for i := 0; i < ctl.n; i++ {
		cfg.StartCtrlerServer(ctl, i)
	}
	ctl.ck = cfg.ctrlerclerk(ctl)
	return ctl
}

var ncpu_once sync.Once

func make_config(t *testing.T, n int, unreliable bool, maxraftstate int) *config {
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.maxraftstate = maxraftstate
	cfg.net = labrpc.MakeNetwork()
	cfg.start = time.Now()

	// controller and its clerk
	cfg.ctl = cfg.StartCtrlerService()

	cfg.ngroups = 3
	cfg.groups = make([]*group, cfg.ngroups)
	cfg.n = n
	for gi := 0; gi < cfg.ngroups; gi++ {
		gg := &group{}
		cfg.groups[gi] = gg
		gg.gid = 100 + gi
		gg.servers = make([]*ShardKV, cfg.n)
		gg.saved = make([]*raft.Persister, cfg.n)
		gg.endnames = make([][]string, cfg.n)
		gg.mendnames = make([][]string, cfg.ctl.n)
		for i := 0; i < cfg.n; i++ {
			cfg.StartServer(gi, i)
		}
	}

	cfg.clerks = make(map[*Clerk][]string)
	cfg.nextClientId = cfg.n + 1000 // client ids start 1000 above the highest serverid

	cfg.net.Reliable(!unreliable)

	// 启动监控服务
	cfg.startMonitor()

	return cfg
}

// 启动监控服务
func (cfg *config) startMonitor() {
	if cfg.monitor != nil {
		return // 已经启动
	}

	cfg.monitor = NewMonitor(8080)

	// 启动 Web 服务器
	go cfg.monitor.StartWebServer()

	// 等待一点时间确保服务器启动
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("监控服务器启动在 http://localhost:8080\n")
}

// 停止监控服务
func (cfg *config) stopMonitor() {
	if cfg.monitor == nil {
		return
	}

	// 将监控器设为 nil，停止使用
	cfg.monitor = nil
}
