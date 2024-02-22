package store

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"time"
)

type cacheContinueToken struct {
	// Nonce present only to make forging continue tokens inconvenient so that
	// they may remain opquqe for future store implementations. Not a secuirty
	// measure.
	Nonce  int64
	Offset int32
}

func decodeCacheContinue(token string) (int, error) {
	out, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return 0, fmt.Errorf("invalid continue token: %s", err)
	}
	buf := bytes.NewReader(out)
	var container cacheContinueToken
	err = binary.Read(buf, binary.LittleEndian, &container)
	return int(container.Offset), err
}

func encodeCacheContinue(offset int) string {
	token := cacheContinueToken{
		Nonce:  time.Now().Unix(),
		Offset: int32(offset),
	}
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, token)
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}
