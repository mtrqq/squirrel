package binary

type BinarySerializable interface {
	ParseBinary(buffer []byte) (int, error)
	// EncodeBinary() ([]byte, error)
	PutBinary(buffer []byte) (int, error)
}

type BinaryExactSerializable interface {
	ParseBinaryExact(buffer []byte) error
	// EncodeBinary() ([]byte, error)
	PutBinaryExact(buffer []byte) error
}
