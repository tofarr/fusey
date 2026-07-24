package journal

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/tofarr/fusey/internal/index"
)

// Dump writes the journal entries to w in a human-readable format:
// one line per entry, fixed-width seq, RFC3339Nano timestamp, op name, and
// the relevant fields. Example:
//
//   seq=000000001 ts=2026-06-05T10:00:00.000000000Z op=CreateInode ino=2 type=Regular mode=0644
//   seq=000000002 ts=2026-06-05T10:00:00.000000000Z op=AddDirEntry parent=1 name="hello.txt" child=2
//   ...
//
// The format is stable (no embedded newlines, fields separated by single
// spaces within a line) so a downstream tool can pipe the output to grep
// or awk if it wants to.
func Dump(w io.Writer, es []Entry) error {
	// Sort by seq (defensive; in practice entries are already in order).
	sorted := make([]Entry, len(es))
	copy(sorted, es)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Seq < sorted[j].Seq })

	for _, e := range sorted {
		if _, err := fmt.Fprintf(w, "seq=%09d ts=%s op=%s",
			e.Seq, formatTS(e.Ts), e.Op.Kind()); err != nil {
			return err
		}
		if rest, err := describeOp(e.Op); err != nil {
			return err
		} else if rest != "" {
			if _, err := fmt.Fprint(w, " "+rest); err != nil {
				return err
			}
		}
		if _, err := io.WriteString(w, "\n"); err != nil {
			return err
		}
	}
	return nil
}

// DumpJSON writes the journal entries as a JSON array. Each entry has the
// shape {"seq":N, "ts":N, "op":{"kind":"...", "fields":{...}}}. The fields
// map mirrors the on-disk CBOR shape (string field names, not integer
// keys) so downstream consumers don't need to know about the wire format.
func DumpJSON(w io.Writer, es []Entry) error {
	out := make([]map[string]interface{}, 0, len(es))
	for _, e := range es {
		out = append(out, map[string]interface{}{
			"seq": e.Seq,
			"ts":  e.Ts,
			"op": map[string]interface{}{
				"kind":   e.Op.Kind(),
				"fields": opFields(e.Op),
			},
		})
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

// formatTS renders a ns-precision Unix timestamp as RFC3339Nano UTC.
func formatTS(ts int64) string {
	return time.Unix(0, ts).UTC().Format(time.RFC3339Nano)
}

// describeOp renders the "field=value field=value ..." tail of a dump line.
// Returns an empty string if the op has no fields worth showing (currently
// none, but kept for future).
func describeOp(o Op) (string, error) {
	var parts []string
	switch {
	case o.CreateInode != nil:
		v := o.CreateInode
		parts = append(parts,
			fmt.Sprintf("ino=%d", v.Ino),
			fmt.Sprintf("type=%s", fileTypeString(v.FileType)),
			fmt.Sprintf("mode=%04o", v.Mode),
			fmt.Sprintf("uid=%d", v.UID),
			fmt.Sprintf("gid=%d", v.GID),
		)
	case o.DeleteInode != nil:
		parts = append(parts, fmt.Sprintf("ino=%d", o.DeleteInode.Ino))
	case o.AddDirEntry != nil:
		v := o.AddDirEntry
		parts = append(parts,
			fmt.Sprintf("parent=%d", v.ParentIno),
			fmt.Sprintf("name=%s", quote(v.Name)),
			fmt.Sprintf("child=%d", v.ChildIno),
		)
	case o.RemoveDirEntry != nil:
		v := o.RemoveDirEntry
		parts = append(parts,
			fmt.Sprintf("parent=%d", v.ParentIno),
			fmt.Sprintf("name=%s", quote(v.Name)),
		)
	case o.Rename != nil:
		v := o.Rename
		parts = append(parts,
			fmt.Sprintf("from=%d:%s", v.SrcParent, quote(v.SrcName)),
			fmt.Sprintf("to=%d:%s", v.DstParent, quote(v.DstName)),
		)
	case o.SetAttr != nil:
		v := o.SetAttr
		parts = append(parts,
			fmt.Sprintf("ino=%d", v.Ino),
			fmt.Sprintf("mode=%04o", v.Mode),
			fmt.Sprintf("uid=%d", v.UID),
			fmt.Sprintf("gid=%d", v.GID),
			fmt.Sprintf("size=%d", v.Size),
		)
	case o.WriteExtent != nil:
		v := o.WriteExtent
		parts = append(parts,
			fmt.Sprintf("ino=%d", v.Ino),
			fmt.Sprintf("chunk=%s", v.Extent.ChunkID),
			fmt.Sprintf("chunkOff=%d", v.Extent.ChunkOffset),
			fmt.Sprintf("fileOff=%d", v.Extent.FileOffset),
			fmt.Sprintf("len=%d", v.Extent.Length),
		)
	case o.SetSymlink != nil:
		v := o.SetSymlink
		parts = append(parts,
			fmt.Sprintf("ino=%d", v.Ino),
			fmt.Sprintf("target=%s", quote(v.Target)),
		)
	case o.SetXattr != nil:
		v := o.SetXattr
		parts = append(parts,
			fmt.Sprintf("ino=%d", v.Ino),
			fmt.Sprintf("name=%s", quote(v.Name)),
			fmt.Sprintf("value=%s", quote(v.Value)),
		)
	case o.RemoveXattr != nil:
		v := o.RemoveXattr
		parts = append(parts,
			fmt.Sprintf("ino=%d", v.Ino),
			fmt.Sprintf("name=%s", quote(v.Name)),
		)
	}
	return strings.Join(parts, " "), nil
}

// opFields returns a map suitable for JSON output. Mirrors describeOp.
func opFields(o Op) map[string]interface{} {
	switch {
	case o.CreateInode != nil:
		v := o.CreateInode
		return map[string]interface{}{
			"ino": v.Ino, "type": fileTypeString(v.FileType), "mode": v.Mode,
			"uid": v.UID, "gid": v.GID, "nlink": v.Nlink, "rdev": v.Rdev,
			"atime": v.Atime, "mtime": v.Mtime, "ctime": v.Ctime,
		}
	case o.DeleteInode != nil:
		return map[string]interface{}{"ino": o.DeleteInode.Ino}
	case o.AddDirEntry != nil:
		v := o.AddDirEntry
		return map[string]interface{}{"parent": v.ParentIno, "name": v.Name, "child": v.ChildIno}
	case o.RemoveDirEntry != nil:
		v := o.RemoveDirEntry
		return map[string]interface{}{"parent": v.ParentIno, "name": v.Name}
	case o.Rename != nil:
		v := o.Rename
		return map[string]interface{}{
			"srcParent": v.SrcParent, "srcName": v.SrcName,
			"dstParent": v.DstParent, "dstName": v.DstName,
		}
	case o.SetAttr != nil:
		v := o.SetAttr
		return map[string]interface{}{
			"ino": v.Ino, "mode": v.Mode, "uid": v.UID, "gid": v.GID,
			"size": v.Size, "atime": v.Atime, "mtime": v.Mtime, "ctime": v.Ctime,
			"nlink": v.Nlink, "rdev": v.Rdev,
		}
	case o.WriteExtent != nil:
		v := o.WriteExtent
		return map[string]interface{}{
			"ino": v.Ino,
			"extent": map[string]interface{}{
				"chunkId": v.Extent.ChunkID, "chunkOffset": v.Extent.ChunkOffset,
				"length": v.Extent.Length, "fileOffset": v.Extent.FileOffset,
			},
		}
	case o.SetSymlink != nil:
		v := o.SetSymlink
		return map[string]interface{}{"ino": v.Ino, "target": v.Target}
	case o.SetXattr != nil:
		v := o.SetXattr
		return map[string]interface{}{"ino": v.Ino, "name": v.Name, "value": v.Value}
	case o.RemoveXattr != nil:
		v := o.RemoveXattr
		return map[string]interface{}{"ino": v.Ino, "name": v.Name}
	}
	return nil
}

func fileTypeString(ft index.FileType) string {
	return ft.String()
}

// quote double-quotes a string, escaping any embedded quotes. Used to
// keep the human-readable output unambiguous when names contain spaces.
func quote(s string) string {
	if !strings.ContainsAny(s, " \t\"") {
		return s
	}
	return `"` + strings.ReplaceAll(s, `"`, `\"`) + `"`
}
