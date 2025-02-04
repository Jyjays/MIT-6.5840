package kvraft

type kvStateMachine struct {
	kvData map[string]string // 键值存储
}

func (kv *kvStateMachine) put(key string, value string) {
	kv.kvData[key] = value
}

func (kv *kvStateMachine) get(key string) string {
	return kv.kvData[key]
}

func (kv *kvStateMachine) append(key string, value string) {
	kv.kvData[key] += value
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
	_, ok := kv.kvData[key]
	return ok
}

func newKVStateMachine() *kvStateMachine {
	return &kvStateMachine{
		kvData: make(map[string]string),
	}
}
