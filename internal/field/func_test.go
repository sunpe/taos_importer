package field

import (
	"strings"
	"testing"
)

func TestDatetimeCache_Len(t *testing.T) {
	s := "A900957.SZ"
	t.Log(s[1:strings.Index(s, ".")])
}
