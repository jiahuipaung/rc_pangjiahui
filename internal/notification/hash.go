package notification

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

func RequestHash(destinationID string, payload json.RawMessage) ([32]byte, error) {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return [32]byte{}, fmt.Errorf("decode payload: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return [32]byte{}, fmt.Errorf("decode trailing payload: %w", err)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return [32]byte{}, fmt.Errorf("encode canonical payload: %w", err)
	}
	input := make([]byte, 0, len(destinationID)+1+len(canonical))
	input = append(input, destinationID...)
	input = append(input, 0)
	input = append(input, canonical...)
	return sha256.Sum256(input), nil
}
