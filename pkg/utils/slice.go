package utils

import "unsafe"

// RemoteItemAt removes the item at the specified index from the slice by
// replacing it with the last item and returning the shortened slice.
func RemoteItemAt[T any](items []T, index int) []T {
	var zero T
	items[index] = items[len(items)-1]
	items[len(items)-1] = zero
	return items[:len(items)-1]
}

// StringTakeOverByteArray converts a byte array to a string without making a copy.
// The caller must ensure that the byte array provided is not modified after this call.
func StringTakeOverByteArray(data []byte) string {
	return unsafe.String(unsafe.SliceData(data), len(data))
}

// ByteArrayFromString converts a string to a byte array without making a copy.
// The caller must ensure that the returned byte array is not modified after this call.
func ByteArrayFromString(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
