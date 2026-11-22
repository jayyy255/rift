package names

import (
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	for _, ok := range []string{"orders", "a", "Orders.v2", "under_score", "dash-ed", "9lives", strings.Repeat("x", MaxLen)} {
		if err := Validate("topic", ok); err != nil {
			t.Errorf("Validate(%q) = %v", ok, err)
		}
	}
	for _, bad := range []string{"", ".", "..", ".hidden", "a/b", "a b", "ü", strings.Repeat("x", MaxLen+1), "a\x00"} {
		if err := Validate("topic", bad); err == nil {
			t.Errorf("Validate(%q) accepted", bad)
		}
	}
}
