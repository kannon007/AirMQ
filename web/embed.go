package web

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var distFS embed.FS

// GetFS returns an fs.FS pointing to the root of the embedded web distribution.
func GetFS() (fs.FS, error) {
	return fs.Sub(distFS, "dist")
}
