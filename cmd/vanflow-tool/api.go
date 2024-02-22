package main

import (
	"encoding/json"

	"github.com/c-kruse/vanflow"
	"github.com/c-kruse/vanflow/store"
)

type RecordsResponse struct {
	Data  []store.Entry `json:"data"`
	Error string        `json:"error,omitempty"`
	Refs  []string      `json:"refs,omitempty"`
}

type ReplaceResponse struct {
	Error  string `json:"error,omitempty"`
	Status string `json:"status,omitempty"`
}

type ReplaceRequest struct {
	Data []PartialEntry `json:"data"`
}

type PartialEntry struct {
	Meta     store.Metadata
	TypeMeta vanflow.TypeMeta
	// don't unmarshal until we have the type info from Metadata
	Record json.RawMessage
}
