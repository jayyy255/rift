//go:build !unix

package broker

import "errors"

func statDisk(string) (DiskStats, error) {
	return DiskStats{}, errors.New("disk usage is not supported on this platform")
}
