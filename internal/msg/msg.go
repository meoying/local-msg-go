package msg

// Msg 根据需要添加字段，例如 metadata 之类的东西
type Msg struct {
	Partition int32
	// Key 一般都建议你传递这个字段，用于排查问题
	Key     string
	Topic   string
	Content []byte
}
