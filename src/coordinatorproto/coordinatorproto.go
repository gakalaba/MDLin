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
	ShardId    int32
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

type ThisShardConnectedArgs struct {
}

type ThisShardConnectedReply struct {
}
