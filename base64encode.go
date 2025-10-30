package main

import (
	"encoding/base64"
	"fmt"
)

func main() {
	enc := "cGFzc3dvcmQxMjM=" // "password123" in base64
	data, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		fmt.Println("decode error:", err)
		return
	}
	fmt.Println("decoded:", string(data))
}
