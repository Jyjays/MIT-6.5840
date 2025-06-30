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
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background-color: #f0f2f5; }
        .container { max-width: 1400px; margin: 0 auto; }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 25px; border-radius: 12px; margin-bottom: 25px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); }
        .main-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 25px; min-height: 70vh; }
        .panel { background: white; padding: 25px; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.08); border: 1px solid rgba(0,0,0,0.05); }
        .server-panel { overflow-y: auto; max-height: 75vh; }
        .events-panel { display: flex; flex-direction: column; }
        .server-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 18px; }
        .server-card { 
            background: linear-gradient(145deg, #f8f9fa 0%, #e9ecef 100%); 
            border: 1px solid #dee2e6; 
            border-radius: 8px; 
            padding: 18px; 
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        .server-card:hover { transform: translateY(-2px); box-shadow: 0 6px 25px rgba(0,0,0,0.1); }
        .server-card.leader { border-left: 5px solid #28a745; background: linear-gradient(145deg, #d4edda 0%, #c3e6cb 100%); }
        .server-card.follower { border-left: 5px solid #6c757d; }
        .shard { display: inline-block; margin: 3px; padding: 6px 10px; border-radius: 6px; font-size: 12px; font-weight: 500; }
        .shard.serving { background-color: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .shard.pulling { background-color: #fff3cd; color: #856404; border: 1px solid #ffeaa7; }
        .shard.sending { background-color: #cce5ff; color: #004085; border: 1px solid #b3d7ff; }
        .shard.gcing { background-color: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
        .events-container { 
            flex: 1; 
            min-height: 65vh; 
            max-height: 65vh; 
            overflow-y: auto; 
            border: 2px solid #e9ecef; 
            border-radius: 8px; 
            padding: 15px; 
            background: #fafbfc;
        }
        .event { 
            padding: 12px 15px; 
            margin: 6px 0; 
            border-radius: 8px; 
            font-size: 14px; 
            border-left: 4px solid #ddd;
            background: white;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
            transition: transform 0.1s ease;
        }
        .event:hover { transform: translateX(3px); }
        .event.raft { background: linear-gradient(145deg, #e3f2fd 0%, #bbdefb 100%); border-left-color: #2196f3; }
        .event.shard { background: linear-gradient(145deg, #f3e5f5 0%, #e1bee7 100%); border-left-color: #9c27b0; }
        .event.config { background: linear-gradient(145deg, #e8f5e8 0%, #c8e6c9 100%); border-left-color: #4caf50; }
        .event.client { background: linear-gradient(145deg, #fff3e0 0%, #ffcc02 100%); border-left-color: #ff9800; }
        .timestamp { color: #666; font-size: 11px; opacity: 0.8; margin-bottom: 4px; }
        .status-indicator { display: inline-block; width: 12px; height: 12px; border-radius: 50%; margin-right: 8px; }
        .status-indicator.online { background-color: #28a745; box-shadow: 0 0 8px rgba(40, 167, 69, 0.4); }
        .status-indicator.offline { background-color: #dc3545; box-shadow: 0 0 8px rgba(220, 53, 69, 0.4); }
        h1, h2, h3 { margin-top: 0; }
        h2 { color: #495057; border-bottom: 2px solid #e9ecef; padding-bottom: 10px; margin-bottom: 20px; }
        .refresh-btn { 
            background: linear-gradient(145deg, #007bff 0%, #0056b3 100%); 
            color: white; 
            border: none; 
            padding: 10px 20px; 
            border-radius: 6px; 
            cursor: pointer; 
            font-weight: 500;
            transition: all 0.2s ease;
        }
        .refresh-btn:hover { transform: translateY(-2px); box-shadow: 0 4px 15px rgba(0, 123, 255, 0.3); }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }
        .stat-card { 
            background: linear-gradient(145deg, #fff 0%, #f8f9fa 100%); 
            padding: 20px; 
            border-radius: 8px; 
            text-align: center; 
            border: 1px solid #e9ecef;
        }
        .stat-number { font-size: 2em; font-weight: bold; color: #495057; }
        .stat-label { color: #6c757d; margin-top: 5px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 ShardKV 分布式监控面板</h1>
            <p>实时监控 ShardKV 集群的状态、分片迁移和同步复制过程</p>
            <button class="refresh-btn" onclick="refreshData()">🔄 刷新数据</button>
        </div>
        
        <div class="main-grid">
            <div class="panel server-panel">
                <h2>🖥️ 服务器状态</h2>
                <div id="servers" class="server-grid"></div>
            </div>
            
            <div class="panel events-panel">
                <h2>📋 最新事件流</h2>
                <div id="events" class="events-container"></div>
            </div>
        </div>
        
        <div class="panel" style="margin-top: 25px;">
            <h2>📊 集群统计概览</h2>
            <div id="statistics" class="stats-grid"></div>
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
                        '<div class="stat-card">' +
                            '<div class="stat-number">' + data.totalServers + '</div>' +
                            '<div class="stat-label">总服务器数</div>' +
                        '</div>' +
                        '<div class="stat-card">' +
                            '<div class="stat-number">' + data.leaderCount + '</div>' +
                            '<div class="stat-label">Leader数量</div>' +
                        '</div>' +
                        '<div class="stat-card">' +
                            '<div class="stat-number">' + data.activeShards + '</div>' +
                            '<div class="stat-label">活跃分片</div>' +
                        '</div>' +
                        '<div class="stat-card">' +
                            '<div class="stat-number">' + data.migrationShards + '</div>' +
                            '<div class="stat-label">迁移中分片</div>' +
                        '</div>';
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
