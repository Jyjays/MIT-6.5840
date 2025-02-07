package shardctrler

type StateMachine struct {
	configs []Config // indexed by config num
}

// The very first configuration should be numbered zero,
// and contains no groups, and all shards are assigned to GID 0.
func NewStateMachine() *StateMachine {
	sm := StateMachine{}
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	return &sm
}
func (sm *StateMachine) copy() StateMachine {
	smCopy := StateMachine{}
	smCopy.configs = make([]Config, len(sm.configs))
	copy(smCopy.configs, sm.configs)
	return smCopy
}
