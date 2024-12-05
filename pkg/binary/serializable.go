package binary

type Serializable interface {
	ParseBinary(buffer []byte) (int, error)
	EncodeBinary() ([]byte, error)
	PutBinary(buffer []byte) (int, error)
	ByteSizeBinary() int64 // TODO: questionable
}
