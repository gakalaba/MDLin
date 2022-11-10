package coordinatorproto

type RegisterArgs struct {
	Addr string
	Port int
}

type RegisterReply struct {
	MasterId int
	MasterList  []string
	Ready     bool
}

type GetShardLeaderListArgs struct {
}

type GetShardLeaderListReply struct {
	LeaderList []string
	Ready      bool
}
