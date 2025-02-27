package masterproto

type RegisterArgs struct {
	Addr string
	Port int
	RpcPort int
}

type RegisterReply struct {
	ReplicaId int
	NodeList  []string
	Ready     bool
}

type RegisterShardsArgs struct {
	ShardList []string
}

type RegisterShardsReply struct {
}

type GetLeaderArgs struct {
}

type GetLeaderReply struct {
	LeaderId   int
	LeaderAddr string
}

type GetReplicaListArgs struct {
}

type GetReplicaListReply struct {
	ReplicaList []string
	Ready       bool
}

type GetShardListArgs struct {
}

type GetShardListReply struct {
	ShardId   int32
	ShardList []string
}

type ShardReadyArgs struct {
}

type ShardReadyReply struct {
}
