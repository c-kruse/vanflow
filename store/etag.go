package store

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"

	"github.com/c-kruse/vanflow/store/internal"
)

type storeConditionKey string
type ifMatch string

const condKey = storeConditionKey("condition")

type cacheETagToken struct {
	Version   int64
	UpdatedAt int64
}

func getETag(cc internal.VersionContainer[Entry]) string {
	entry := cc.Resource
	token := cacheETagToken{
		Version:   cc.Version,
		UpdatedAt: entry.Meta.UpdatedAt.Unix(),
	}
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, token)
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func decodeETag(etag string) (cacheETagToken, error) {
	out, err := base64.StdEncoding.DecodeString(etag)
	if err != nil {
		return cacheETagToken{}, fmt.Errorf("invalid ETag: %s", err)
	}
	buf := bytes.NewReader(out)
	var container cacheETagToken
	err = binary.Read(buf, binary.LittleEndian, &container)
	return container, err
}

// WithIfMatch returns a new context loaded with an ETag for an If-Match
// conditional
func WithIfMatch(ctx context.Context, etag string) context.Context {
	return context.WithValue(ctx, condKey, ifMatch(etag))
}

func getConditional(ctx context.Context) (internal.Cond[Entry], error) {
	condVal := ctx.Value(condKey)
	if condVal == nil {
		return nil, nil
	}
	etag, ok := condVal.(ifMatch)
	if !ok {
		return nil, nil
	}
	token, err := decodeETag(string(etag))
	if err != nil {
		return nil, err
	}
	return internal.WhenVersionMatches[Entry](token.Version), nil
}
