package main

import (
	"fmt"
	"strings"
)

type stringsFlag []string

func (sf *stringsFlag) Set(s string) error {
	for _, part := range strings.Split(s, ",") {
		if part != "" {
			*sf = append(*sf, part)
		}
	}
	return nil
}

func (sf *stringsFlag) String() string {
	return fmt.Sprintf("%q", strings.Join(*sf, ","))
}
