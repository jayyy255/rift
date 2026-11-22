// Package names validates user-chosen identifiers such as topic and
// consumer-group names, which are also used as file names on disk.
package names

import (
	"fmt"
	"regexp"
)

// MaxLen is the longest accepted name.
const MaxLen = 200

var valid = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9._-]*$`)

// Validate reports whether name is 1-200 characters from [A-Za-z0-9._-] and
// does not start with '.', which is reserved for internal files.
func Validate(kind, name string) error {
	if name == "" {
		return fmt.Errorf("%s name is required", kind)
	}
	if len(name) > MaxLen {
		return fmt.Errorf("%s name is %d characters; the limit is %d", kind, len(name), MaxLen)
	}
	if !valid.MatchString(name) {
		return fmt.Errorf("%s name %q must use only letters, digits, '.', '_' and '-', and must not start with '.'", kind, name)
	}
	return nil
}
