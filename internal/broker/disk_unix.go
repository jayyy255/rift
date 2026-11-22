//go:build unix

package broker

import "golang.org/x/sys/unix"

func statDisk(path string) (DiskStats, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return DiskStats{}, err
	}
	bs := uint64(st.Bsize) //nolint:unconvert // Bsize's type differs across platforms
	return DiskStats{Total: st.Blocks * bs, Available: st.Bavail * bs}, nil
}
