package utils

import "unsafe"

func StringsToBytes(s string) []byte {
	if s == "" {
		return make([]byte, 0)
	}

	return *(*[]byte)(unsafe.Pointer(&s))
}

func BytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}

	return *(*string)(unsafe.Pointer(&b))
}
