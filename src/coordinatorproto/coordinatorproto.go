package coordinatorproto

type RegisterArgs struct {
	Addr string
	Port int
}

type RegisterReply struct {
	ReplicaId int
	NodeList  []string
	Ready     bool
}

type GetShardLeaderListArgs struct {
}

type GetShardLeaderListReply struct {
	LeaderList []string
	Ready      bool
}
