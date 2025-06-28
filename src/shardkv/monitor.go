package shardkv

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"6.5840/shardctrler"
)

// MonitorEvent 记录监控事件
type MonitorEvent struct {
	Timestamp   time.Time              `json:"timestamp"`
	EventType   string                 `json:"eventType"`
	GroupID     int                    `json:"groupId"`
	ServerID    int                    `json:"serverId"`
	Description string                 `json:"description"`
	Details     map[string]interface{} `json:"details,omitempty"`
}

// ShardStatus 分片状态信息
type ShardStatus struct {
	ShardID  int        `json:"shardId"`
	State    ShardState `json:"state"`
	GroupID  int        `json:"groupId"`
	KeyCount int        `json:"keyCount"`
}

// ServerStatus 服务器状态信息
type ServerStatus struct {
	GroupID       int                    `json:"groupId"`
	ServerID      int                    `json:"serverId"`
	IsLeader      bool                   `json:"isLeader"`
	CurrentTerm   int                    `json:"currentTerm"`
	CurrentConfig shardctrler.Config     `json:"currentConfig"`
	Shards        map[int]ShardStatus    `json:"shards"`
	LastOperation map[int64]ReplyContext `json:"lastOperation"`
	LastApplied   int                    `json:"lastApplied"`
}

// Monitor ShardKV监控器
type Monitor struct {
	mu          sync.RWMutex
	events      []MonitorEvent
	serverStats map[string]*ServerStatus
	maxEvents   int
	port        int
}

// NewMonitor 创建新的监控器
func NewMonitor(port int) *Monitor {
	return &Monitor{
		events:      make([]MonitorEvent, 0),
		serverStats: make(map[string]*ServerStatus),
		maxEvents:   1000, // 最多保存1000个事件
		port:        port,
	}
}

// 全局监控器实例
var globalMonitor *Monitor
var once sync.Once

// GetMonitor 获取全局监控器实例
func GetMonitor() *Monitor {
	once.Do(func() {
		globalMonitor = NewMonitor(8080)
		go globalMonitor.StartWebServer()
	})
	return globalMonitor
}

// LogEvent 记录事件
func (m *Monitor) LogEvent(eventType string, groupID, serverID int, description string, details map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()

	event := MonitorEvent{
		Timestamp:   time.Now(),
		EventType:   eventType,
		GroupID:     groupID,
		ServerID:    serverID,
		Description: description,
		Details:     details,
	}

	m.events = append(m.events, event)

	// 保持事件数量在限制内
	if len(m.events) > m.maxEvents {
		m.events = m.events[len(m.events)-m.maxEvents:]
	}
}

// UpdateServerStatus 更新服务器状态
func (m *Monitor) UpdateServerStatus(kv *ShardKV) {
	serverKey := fmt.Sprintf("%d-%d", kv.gid, kv.me)

	// 获取Raft状态
	currentTerm, isLeader := kv.rf.GetState()

	// 收集分片状态
	shards := make(map[int]ShardStatus)
	kv.mu.RLock()
	for sid, shard := range kv.stateMachine.Shards {
		keyCount := 0
		if shard != nil {
			keyCount = len(shard.KvData)
		}
		shards[sid] = ShardStatus{
			ShardID:  sid,
			State:    shard.State,
			GroupID:  kv.currentConfig.Shards[sid],
			KeyCount: keyCount,
		}
	}
	kv.mu.RUnlock()

	status := &ServerStatus{
		GroupID:       kv.gid,
		ServerID:      kv.me,
		IsLeader:      isLeader,
		CurrentTerm:   currentTerm,
		CurrentConfig: kv.currentConfig,
		Shards:        shards,
		LastOperation: kv.lastOperation,
		LastApplied:   kv.lastApplied,
	}

	// 使用锁保护对 serverStats map 的访问
	m.mu.Lock()
	m.serverStats[serverKey] = status
	m.mu.Unlock()
}

// GetEvents 获取事件列表
func (m *Monitor) GetEvents() []MonitorEvent {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 返回最近的事件
	events := make([]MonitorEvent, len(m.events))
	copy(events, m.events)
	return events
}

// GetServerStats 获取服务器状态
func (m *Monitor) GetServerStats() map[string]*ServerStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]*ServerStatus)
	for k, v := range m.serverStats {
		stats[k] = v
	}
	return stats
}

// StartWebServer 启动Web服务器
func (m *Monitor) StartWebServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", m.handleIndex)
	mux.HandleFunc("/api/events", m.handleEvents)
	mux.HandleFunc("/api/servers", m.handleServers)
	mux.HandleFunc("/api/status", m.handleStatus)

	fmt.Printf("ShardKV Monitor started at http://localhost:%d\n", m.port)
	http.ListenAndServe(fmt.Sprintf(":%d", m.port), mux)
}

// HTTP处理函数
func (m *Monitor) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	html := `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ShardKV Monitor</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
        .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        .panel { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .server-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 15px; }
        .server-card { background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 6px; padding: 15px; }
        .server-card.leader { border-left: 4px solid #28a745; }
        .server-card.follower { border-left: 4px solid #6c757d; }
        .shard { display: inline-block; margin: 2px; padding: 4px 8px; border-radius: 4px; font-size: 12px; }
        .shard.serving { background-color: #d4edda; color: #155724; }
        .shard.pulling { background-color: #fff3cd; color: #856404; }
        .shard.sending { background-color: #cce5ff; color: #004085; }
        .shard.gcing { background-color: #f8d7da; color: #721c24; }
        .event { padding: 8px; margin: 4px 0; border-radius: 4px; font-size: 14px; }
        .event.raft { background-color: #e3f2fd; }
        .event.shard { background-color: #f3e5f5; }
        .event.config { background-color: #e8f5e8; }
        .event.client { background-color: #fff3e0; }
        .timestamp { color: #666; font-size: 12px; }
        .status-indicator { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 5px; }
        .status-indicator.online { background-color: #28a745; }
        .status-indicator.offline { background-color: #dc3545; }
        h1, h2, h3 { margin-top: 0; }
        .refresh-btn { background: #007bff; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer; }
        .refresh-btn:hover { background: #0056b3; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 ShardKV 分布式监控面板</h1>
            <p>实时监控 ShardKV 集群的状态、分片迁移和同步复制过程</p>
            <button class="refresh-btn" onclick="refreshData()">🔄 刷新数据</button>
        </div>
        
        <div class="grid">
            <div class="panel">
                <h2>🖥️ 服务器状态</h2>
                <div id="servers" class="server-grid"></div>
            </div>
            
            <div class="panel">
                <h2>📋 最新事件</h2>
                <div id="events" style="max-height: 500px; overflow-y: auto;"></div>
            </div>
        </div>
        
        <div class="panel" style="margin-top: 20px;">
            <h2>📊 集群统计</h2>
            <div id="statistics"></div>
        </div>
    </div>

    <script>
        function refreshData() {
            fetchServers();
            fetchEvents();
        }

        function fetchServers() {
            fetch('/api/servers')
                .then(response => response.json())
                .then(data => {
                    const container = document.getElementById('servers');
                    container.innerHTML = '';
                    
                    Object.values(data).forEach(server => {
                        const serverDiv = document.createElement('div');
                        serverDiv.className = 'server-card ' + (server.isLeader ? 'leader' : 'follower');
                        
                        let shardsHtml = '';
                        Object.values(server.shards || {}).forEach(shard => {
                            const stateClass = shard.state.toString().toLowerCase();
                            shardsHtml += '<span class="shard ' + stateClass + '">' + 
                                         'S' + shard.shardId + '(' + shard.keyCount + ')' + '</span>';
                        });
                        
                        serverDiv.innerHTML = 
                            '<h3><span class="status-indicator online"></span>' +
                            'Group ' + server.groupId + ' Server ' + server.serverId +
                            (server.isLeader ? ' 👑' : '') + '</h3>' +
                            '<p><strong>Term:</strong> ' + server.currentTerm + '</p>' +
                            '<p><strong>Config:</strong> ' + server.currentConfig.num + '</p>' +
                            '<p><strong>Last Applied:</strong> ' + server.lastApplied + '</p>' +
                            '<p><strong>Shards:</strong></p>' +
                            '<div>' + shardsHtml + '</div>';
                        
                        container.appendChild(serverDiv);
                    });
                })
                .catch(err => console.error('Error fetching servers:', err));
        }

        function fetchEvents() {
            fetch('/api/events')
                .then(response => response.json())
                .then(data => {
                    const container = document.getElementById('events');
                    container.innerHTML = '';
                    
                    data.slice(-50).reverse().forEach(event => {
                        const eventDiv = document.createElement('div');
                        eventDiv.className = 'event ' + event.eventType.toLowerCase();
                        
                        const timestamp = new Date(event.timestamp).toLocaleTimeString();
                        eventDiv.innerHTML = 
                            '<div class="timestamp">' + timestamp + '</div>' +
                            '<strong>[G' + event.groupId + ':S' + event.serverId + '] ' + 
                            event.eventType + ':</strong> ' + event.description;
                        
                        container.appendChild(eventDiv);
                    });
                })
                .catch(err => console.error('Error fetching events:', err));
        }

        function updateStatistics() {
            fetch('/api/status')
                .then(response => response.json())
                .then(data => {
                    const container = document.getElementById('statistics');
                    container.innerHTML = 
                        '<p><strong>总服务器数:</strong> ' + data.totalServers + '</p>' +
                        '<p><strong>Leader数:</strong> ' + data.leaderCount + '</p>' +
                        '<p><strong>活跃分片数:</strong> ' + data.activeShards + '</p>' +
                        '<p><strong>迁移中分片数:</strong> ' + data.migrationShards + '</p>';
                })
                .catch(err => console.error('Error fetching statistics:', err));
        }

        // 初始化并设置定时刷新
        refreshData();
        updateStatistics();
        setInterval(refreshData, 2000);  // 每2秒刷新一次
        setInterval(updateStatistics, 5000);  // 每5秒更新统计
    </script>
</body>
</html>`
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(html))
}

func (m *Monitor) handleEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	events := m.GetEvents()
	json.NewEncoder(w).Encode(events)
}

func (m *Monitor) handleServers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	servers := m.GetServerStats()
	json.NewEncoder(w).Encode(servers)
}

func (m *Monitor) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	servers := m.GetServerStats()
	totalServers := len(servers)
	leaderCount := 0
	activeShards := 0
	migrationShards := 0

	for _, server := range servers {
		if server.IsLeader {
			leaderCount++
		}
		for _, shard := range server.Shards {
			if shard.State == Serving {
				activeShards++
			} else if shard.State == Pulling || shard.State == Sending {
				migrationShards++
			}
		}
	}

	status := map[string]int{
		"totalServers":    totalServers,
		"leaderCount":     leaderCount,
		"activeShards":    activeShards,
		"migrationShards": migrationShards,
	}

	json.NewEncoder(w).Encode(status)
}
