package kvraft

type kvStateMachine struct {
	KvData map[string]string // 键值存储
}

func (kv *kvStateMachine) put(key string, value string) {
	kv.KvData[key] = value
}

func (kv *kvStateMachine) get(key string) string {
	return kv.KvData[key]
}

func (kv *kvStateMachine) append(key string, value string) {
	kv.KvData[key] += value
}

func (kv *kvStateMachine) apply(op Op) {
	switch op.Type {
	case "Put":
		kv.put(op.Key, op.Value)
	case "Append":
		kv.append(op.Key, op.Value)
	}
}

func (kv *kvStateMachine) hasKey(key string) bool {
	_, ok := kv.KvData[key]
	return ok
}

func newKVStateMachine() *kvStateMachine {
	return &kvStateMachine{
		KvData: make(map[string]string),
	}
}
