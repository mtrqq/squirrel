package raw

import (
	"fmt"
)

func PutBytes(buffer []byte, data []byte) (int, error) {
	if len(buffer) < len(data) {
		return 0, fmt.Errorf("insufficient buffer size to put data, got %d, want %d", len(buffer), len(data))
	}

	return copy(buffer, data), nil
}
