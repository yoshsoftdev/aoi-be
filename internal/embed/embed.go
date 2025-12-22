package embed

import (
	"embed"
	"io/fs"
)

//go:embed *
var staticFiles embed.FS

// GetStaticFiles returns the embedded filesystem containing the frontend static files
func GetStaticFiles() fs.FS {
	return staticFiles
}

// GetStaticFS returns the embed.FS directly (for compatibility)
func GetStaticFS() embed.FS {
	return staticFiles
}
