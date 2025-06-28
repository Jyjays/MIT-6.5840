package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
	"6.5840/shardkv"
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("❌ 程序panic: %v\n", r)
		}
	}()

	fmt.Println("🎯 ShardKV 监控演示启动")
	fmt.Println("📊 访问 http://localhost:8080 查看监控界面")
	fmt.Println("🚀 正在创建真实的ShardKV集群...")
	fmt.Println("")

	// 启动监控器
	monitor := shardkv.GetMonitor()
	monitor.LogEvent("SYSTEM", 0, 0, "监控演示启动", map[string]interface{}{
		"message": "正在创建ShardKV集群",
	})

	// 创建配置
	fmt.Println("📋 步骤1: 创建配置...")
	cfg := makeConfig(3, false, -1) // 3个服务器，可靠网络，无快照
	defer cfg.cleanup()

	fmt.Println("✅ ShardKV集群已创建")
	fmt.Println("🎮 开始演示分片操作...")

	// 创建客户端
	fmt.Println("📋 步骤2: 创建客户端...")
	ck := cfg.makeClient(cfg.ctl)
	fmt.Println("✅ 客户端创建成功")

	// 加入第一个组
	cfg.join(0)
	time.Sleep(1 * time.Second)

	// 执行一些操作来展示监控效果
	go func() {
		fmt.Println("📝 开始执行客户端操作...")

		// Put一些键值对
		for i := 0; i < 10; i++ {
			key := "key" + strconv.Itoa(i)
			value := "value" + strconv.Itoa(i)
			ck.Put(key, value)
			fmt.Printf("Put: %s = %s\n", key, value)
			time.Sleep(500 * time.Millisecond)
		}

		time.Sleep(2 * time.Second)

		// 加入第二个组，触发分片迁移
		fmt.Println("🔄 加入新组，触发分片迁移...")
		cfg.join(1)
		time.Sleep(3 * time.Second)

		// 继续执行操作
		for i := 10; i < 20; i++ {
			key := "key" + strconv.Itoa(i)
			value := "value" + strconv.Itoa(i)
			ck.Put(key, value)
			fmt.Printf("Put: %s = %s\n", key, value)
			time.Sleep(300 * time.Millisecond)
		}

		time.Sleep(2 * time.Second)

		// 加入第三个组
		fmt.Println("🔄 加入第三个组...")
		cfg.join(2)
		time.Sleep(3 * time.Second)

		// 进行一些Get操作
		fmt.Println("📖 执行读取操作...")
		for i := 0; i < 20; i++ {
			key := "key" + strconv.Itoa(i)
			value := ck.Get(key)
			fmt.Printf("Get: %s = %s\n", key, value)
			time.Sleep(200 * time.Millisecond)
		}

		time.Sleep(2 * time.Second)

		// 移除一个组，再次触发迁移
		fmt.Println("🚪 移除一个组，触发分片迁移...")
		cfg.leave(1)
		time.Sleep(3 * time.Second)

		// 继续操作验证数据一致性
		fmt.Println("✅ 验证数据一致性...")
		for i := 0; i < 20; i++ {
			key := "key" + strconv.Itoa(i)
			expectedValue := "value" + strconv.Itoa(i)
			actualValue := ck.Get(key)
			if actualValue != expectedValue {
				fmt.Printf("❌ 数据不一致: %s expected=%s actual=%s\n", key, expectedValue, actualValue)
			} else {
				fmt.Printf("✅ 数据一致: %s = %s\n", key, actualValue)
			}
			time.Sleep(100 * time.Millisecond)
		}

		fmt.Println("🎉 演示完成！")
	}()

	fmt.Println("✅ 监控演示已启动，按 Ctrl+C 停止...")

	// 保持程序运行
	for {
		time.Sleep(time.Second)
	}
}

// 以下是从test_test.go中提取的配置相关代码

// 辅助函数：创建客户端端点数组
func makeClientEnds(net *labrpc.Network, names []string) []*labrpc.ClientEnd {
	ends := make([]*labrpc.ClientEnd, len(names))
	for i, name := range names {
		endname := "client-" + name + "-" + strconv.Itoa(rand.Int())
		ends[i] = net.MakeEnd(endname)
		net.Connect(endname, name)
		net.Enable(endname, true)
	}
	return ends
}

type config struct {
	net          *labrpc.Network
	ngroups      int
	groups       []*group
	ctl          *ctrler
	clerks       map[*shardkv.Clerk][]string
	nextClientId int
	maxraftstate int
}

type group struct {
	gid       int
	servers   []*shardkv.ShardKV
	saved     []*raft.Persister
	endnames  [][]string
	mendnames [][]string
}

type ctrler struct {
	n       int
	servers []*shardctrler.ShardCtrler
	names   []string
	ck      *shardctrler.Clerk
}

func makeConfig(n int, unreliable bool, maxraftstate int) *config {
	rand.Seed(time.Now().UnixNano())

	cfg := &config{}
	cfg.maxraftstate = maxraftstate
	cfg.net = labrpc.MakeNetwork()

	// 创建分片控制器
	cfg.ctl = &ctrler{}
	cfg.ctl.n = 3
	cfg.ctl.servers = make([]*shardctrler.ShardCtrler, cfg.ctl.n)
	cfg.ctl.names = make([]string, cfg.ctl.n)

	for i := 0; i < cfg.ctl.n; i++ {
		cfg.ctl.names[i] = "ctl" + strconv.Itoa(i)
		ends := make([]*labrpc.ClientEnd, cfg.ctl.n)
		for j := 0; j < cfg.ctl.n; j++ {
			endname := fmt.Sprintf("ctl%d-%d", i, j)
			ends[j] = cfg.net.MakeEnd(endname)
			cfg.net.Connect(endname, cfg.ctl.names[j])
			cfg.net.Enable(endname, true)
		}

		p := raft.MakePersister()
		cfg.ctl.servers[i] = shardctrler.StartServer(ends, i, p)

		msvc := labrpc.MakeService(cfg.ctl.servers[i])
		rfsvc := labrpc.MakeService(cfg.ctl.servers[i].Raft())
		srv := labrpc.MakeServer()
		srv.AddService(msvc)
		srv.AddService(rfsvc)
		cfg.net.AddServer(cfg.ctl.names[i], srv)
	}

	cfg.ctl.ck = shardctrler.MakeClerk(makeClientEnds(cfg.net, cfg.ctl.names))

	// 创建组
	cfg.ngroups = 3
	cfg.groups = make([]*group, cfg.ngroups)

	for gi := 0; gi < cfg.ngroups; gi++ {
		gg := &group{}
		cfg.groups[gi] = gg
		gg.gid = 100 + gi
		gg.servers = make([]*shardkv.ShardKV, n)
		gg.saved = make([]*raft.Persister, n)
		gg.endnames = make([][]string, n)
		gg.mendnames = make([][]string, n)

		for i := 0; i < n; i++ {
			cfg.startServer(gi, i)
		}
	}

	cfg.clerks = make(map[*shardkv.Clerk][]string)
	cfg.nextClientId = 1000
	cfg.net.Reliable(!unreliable)

	return cfg
}

func (cfg *config) startServer(gi int, i int) {
	gg := cfg.groups[gi]

	// 创建服务器间连接
	gg.endnames[i] = make([]string, len(gg.servers))
	for j := 0; j < len(gg.servers); j++ {
		gg.endnames[i][j] = fmt.Sprintf("srv%d-%d-%d", gi, i, j)
	}

	ends := make([]*labrpc.ClientEnd, len(gg.servers))
	for j := 0; j < len(gg.servers); j++ {
		ends[j] = cfg.net.MakeEnd(gg.endnames[i][j])
		cfg.net.Connect(gg.endnames[i][j], fmt.Sprintf("srv%d-%d", gi, j))
		cfg.net.Enable(gg.endnames[i][j], true)
	}

	// 创建到控制器的连接
	mends := make([]*labrpc.ClientEnd, cfg.ctl.n)
	gg.mendnames[i] = make([]string, cfg.ctl.n)
	for j := 0; j < cfg.ctl.n; j++ {
		gg.mendnames[i][j] = fmt.Sprintf("srv%d-%d-ctl%d", gi, i, j)
		mends[j] = cfg.net.MakeEnd(gg.mendnames[i][j])
		cfg.net.Connect(gg.mendnames[i][j], cfg.ctl.names[j])
		cfg.net.Enable(gg.mendnames[i][j], true)
	}

	if gg.saved[i] != nil {
		gg.saved[i] = gg.saved[i].Copy()
	} else {
		gg.saved[i] = raft.MakePersister()
	}

	gg.servers[i] = shardkv.StartServer(ends, i, gg.saved[i], cfg.maxraftstate,
		gg.gid, mends,
		func(servername string) *labrpc.ClientEnd {
			name := fmt.Sprintf("srv%d-%d-ext-%s", gi, i, servername)
			end := cfg.net.MakeEnd(name)
			cfg.net.Connect(name, servername)
			cfg.net.Enable(name, true)
			return end
		})

	kvsvc := labrpc.MakeService(gg.servers[i])
	// 删除 rfsvc，因为ShardKV没有直接暴露Raft服务
	srv := labrpc.MakeServer()
	srv.AddService(kvsvc)
	cfg.net.AddServer(fmt.Sprintf("srv%d-%d", gi, i), srv)
}

func (cfg *config) cleanup() {
	for gi := 0; gi < cfg.ngroups; gi++ {
		gg := cfg.groups[gi]
		for i := 0; i < len(gg.servers); i++ {
			if gg.servers[i] != nil {
				gg.servers[i].Kill()
			}
		}
	}

	for i := 0; i < cfg.ctl.n; i++ {
		if cfg.ctl.servers[i] != nil {
			cfg.ctl.servers[i].Kill()
		}
	}
}

func (cfg *config) join(gi int) {
	gg := cfg.groups[gi]
	servers := make([]string, len(gg.servers))
	for i := 0; i < len(gg.servers); i++ {
		servers[i] = fmt.Sprintf("srv%d-%d", gi, i)
	}
	cfg.ctl.ck.Join(map[int][]string{gg.gid: servers})
}

func (cfg *config) leave(gi int) {
	gg := cfg.groups[gi]
	cfg.ctl.ck.Leave([]int{gg.gid})
}

func (cfg *config) makeClient(ctrlers *ctrler) *shardkv.Clerk {
	fmt.Println("📋 makeClient: 开始创建客户端...")
	ends := makeClientEnds(cfg.net, ctrlers.names)
	fmt.Printf("📋 makeClient: 创建了 %d 个端点\n", len(ends))

	ck := shardkv.MakeClerk(ends, func(servername string) *labrpc.ClientEnd {
		name := fmt.Sprintf("client-%d-%s", cfg.nextClientId, servername)
		end := cfg.net.MakeEnd(name)
		cfg.net.Connect(name, servername)
		cfg.net.Enable(name, true)
		return end
	})
	fmt.Println("📋 makeClient: MakeClerk完成")

	cfg.clerks[ck] = ctrlers.names
	cfg.nextClientId++
	fmt.Println("📋 makeClient: 客户端创建完成")
	return ck
}
