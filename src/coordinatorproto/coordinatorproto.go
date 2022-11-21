package coordinatorproto

type RegisterArgs struct {
	Addr string
	Port int
}

type RegisterReply struct {
	MasterList []string
	Ready      bool
}

type RegisterLeaderArgs struct {
	LeaderAddr string
	MasterAddr string
}

type RegisterLeaderReply struct {
}

type GetShardLeaderListArgs struct {
}

type GetShardLeaderListReply struct {
	LeaderList []string
}

type RegisterKeyspaceReply struct {
}
