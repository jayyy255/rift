// Package offsets durably stores consumer-group offset checkpoints.
//
// A committed offset is the next offset the group will consume from a
// partition (last processed offset + 1). Each group is one JSON file,
// replaced atomically on every commit.
package offsets

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/jayyy255/rift/internal/fsutil"
	"github.com/jayyy255/rift/internal/names"
)

const fileFormat = 1

type groupFile struct {
	FormatVersion int                         `json:"format_version"`
	Group         string                      `json:"group"`
	Topics        map[string]map[string]int64 `json:"topics"` // topic -> partition -> offset
}

// Entry is one committed offset.
type Entry struct {
	Group     string
	Topic     string
	Partition int32
	Offset    int64
}

// Store holds committed offsets for all groups. It is safe for concurrent use.
type Store struct {
	dir    string
	mu     sync.Mutex
	groups map[string]map[string]map[int32]int64
}

// Open loads all group files from dir, creating it if needed.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, groups: map[string]map[string]map[int32]int64{}}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".tmp") {
			os.Remove(filepath.Join(dir, name)) // interrupted commit; the old file is intact
			continue
		}
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		var gf groupFile
		if err := json.Unmarshal(data, &gf); err != nil {
			return nil, fmt.Errorf("offsets: %s: %w", name, err)
		}
		if gf.FormatVersion != fileFormat || gf.Group+".json" != name {
			return nil, fmt.Errorf("offsets: %s: unsupported or inconsistent file", name)
		}
		topics := map[string]map[int32]int64{}
		for topic, parts := range gf.Topics {
			topics[topic] = map[int32]int64{}
			for p, off := range parts {
				pi, err := strconv.ParseInt(p, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("offsets: %s: bad partition %q", name, p)
				}
				topics[topic][int32(pi)] = off
			}
		}
		s.groups[gf.Group] = topics
	}
	return s, nil
}

// Commit durably stores offsets for a group and topic. It returns only after
// the group file has been fsynced.
func (s *Store) Commit(group, topic string, offsets map[int32]int64) error {
	if err := names.Validate("group", group); err != nil {
		return err
	}
	for p, off := range offsets {
		if p < 0 || off < 0 {
			return fmt.Errorf("offsets: invalid partition %d / offset %d", p, off)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next := cloneGroup(s.groups[group])
	if next[topic] == nil {
		next[topic] = map[int32]int64{}
	}
	for p, off := range offsets {
		next[topic][p] = off
	}
	if err := s.write(group, next); err != nil {
		return err
	}
	s.groups[group] = next
	return nil
}

// Get returns the committed offsets of a group for a topic.
func (s *Store) Get(group, topic string) map[int32]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[int32]int64{}
	for p, off := range s.groups[group][topic] {
		out[p] = off
	}
	return out
}

// DeleteTopic removes a topic's offsets from every group.
func (s *Store) DeleteTopic(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	for group, topics := range s.groups {
		if _, ok := topics[topic]; !ok {
			continue
		}
		next := cloneGroup(topics)
		delete(next, topic)
		if err := s.write(group, next); err != nil {
			errs = append(errs, err)
			continue
		}
		s.groups[group] = next
	}
	return errors.Join(errs...)
}

// Clamp lowers committed offsets above end to end. It is used after recovery
// truncates a partition, so groups do not skip records later written at the
// reused offsets. It returns the groups that were changed.
func (s *Store) Clamp(topic string, partition int32, end int64) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var changed []string
	for group, topics := range s.groups {
		if off, ok := topics[topic][partition]; ok && off > end {
			next := cloneGroup(topics)
			next[topic][partition] = end
			if err := s.write(group, next); err != nil {
				return changed, err
			}
			s.groups[group] = next
			changed = append(changed, group)
		}
	}
	sort.Strings(changed)
	return changed, nil
}

// Snapshot returns every committed offset.
func (s *Store) Snapshot() []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Entry
	for g, topics := range s.groups {
		for t, parts := range topics {
			for p, off := range parts {
				out = append(out, Entry{Group: g, Topic: t, Partition: p, Offset: off})
			}
		}
	}
	return out
}

func (s *Store) write(group string, topics map[string]map[int32]int64) error {
	gf := groupFile{FormatVersion: fileFormat, Group: group, Topics: map[string]map[string]int64{}}
	for t, parts := range topics {
		gf.Topics[t] = map[string]int64{}
		for p, off := range parts {
			gf.Topics[t][strconv.Itoa(int(p))] = off
		}
	}
	data, err := json.MarshalIndent(gf, "", "  ")
	if err != nil {
		return err
	}
	return fsutil.WriteFileAtomic(filepath.Join(s.dir, group+".json"), data)
}

func cloneGroup(in map[string]map[int32]int64) map[string]map[int32]int64 {
	out := make(map[string]map[int32]int64, len(in))
	for t, parts := range in {
		out[t] = make(map[int32]int64, len(parts))
		for p, off := range parts {
			out[t][p] = off
		}
	}
	return out
}
