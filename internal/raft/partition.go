package raft

type Partition struct {
	id  int32
	log *Log
}

func NewPartition(port int16, topic string, partitionId int32) *Partition {
	return &Partition{
		id:  partitionId,
		log: NewLog(port, topic, partitionId),
	}
}
