package platform

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/zeroko-io/runner/internal/config"
	"github.com/zeroko-io/runner/internal/deploy"
	"github.com/zeroko-io/runner/internal/store"
)

func init() {
	// Types Go does not know, for files services commonly serve.
	for ext, typ := range map[string]string{
		".txt": "text/plain; charset=utf-8", ".md": "text/markdown; charset=utf-8", ".csv": "text/csv; charset=utf-8",
		".map": "application/json", ".webmanifest": "application/manifest+json", ".ico": "image/x-icon",
		".woff": "font/woff", ".woff2": "font/woff2", ".ttf": "font/ttf", ".otf": "font/otf",
		".mp3": "audio/mpeg", ".ogg": "audio/ogg", ".wav": "audio/wav", ".mp4": "video/mp4", ".webm": "video/webm",
		".glb": "model/gltf-binary", ".gltf": "model/gltf+json",
	} {
		mime.AddExtensionType(ext, typ)
	}
}

// Site is a deployment's public/ files, which its Assets binding serves.
type Site struct {
	dir   string
	files map[string]Asset // path under public/
}

type Asset struct {
	Type string
	ETag string
}

// Site returns the files of a deployment. They are extracted from the bundle
// once, to /data/assets/<bundle>/.
func (p *Platform) Site(ctx context.Context, serviceID, deploymentID string) (*Site, error) {
	if s, ok := p.sites.Load(deploymentID); ok {
		return s.(*Site), nil
	}
	d, err := p.Store.Deployment(ctx, serviceID, deploymentID)
	if err != nil {
		return nil, err
	}
	m := p.Manifest(d)
	s := &Site{dir: p.assetsDir(d.Bundle), files: map[string]Asset{}}
	if m.Assets > 0 {
		if err := p.extractAssets(d.Bundle, s); err != nil {
			return nil, err
		}
	}
	p.sites.Store(deploymentID, s)
	return s, nil
}

func (p *Platform) assetsDir(bundle string) string {
	return filepath.Join(config.DataDir, "assets", bundle)
}

func (p *Platform) extractAssets(bundle string, s *Site) error {
	b, err := os.ReadFile(filepath.Join(p.bundlesDir, bundle+".json"))
	if err != nil {
		return err
	}
	var modules []deploy.Module
	if err := json.Unmarshal(b, &modules); err != nil {
		return err
	}
	_, statErr := os.Stat(s.dir)
	tmp := s.dir + ".tmp-" + store.NewID()
	defer os.RemoveAll(tmp)
	for _, m := range modules {
		if m.Type != "asset" {
			continue
		}
		name := strings.TrimPrefix(m.Name, deploy.AssetPrefix)
		h := sha256.Sum256(m.Content)
		typ := mime.TypeByExtension(path.Ext(name))
		if typ == "" {
			typ = "application/octet-stream"
		}
		s.files[name] = Asset{Type: typ, ETag: `"` + hex.EncodeToString(h[:12]) + `"`}
		if statErr == nil {
			continue // extracted before
		}
		file := filepath.Join(tmp, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(file, m.Content, 0o600); err != nil {
			return err
		}
	}
	if statErr == nil {
		return nil
	}
	if err := os.Rename(tmp, s.dir); err != nil && !os.IsExist(err) {
		if _, serr := os.Stat(s.dir); serr != nil { // not another request extracting the same
			return err
		}
	}
	return nil
}

// Find maps a request path to a file: /x serves x, x.html or x/index.html,
// and a path ending in / its index.html.
func (s *Site) Find(urlPath string) (string, Asset, bool) {
	if len(s.files) == 0 {
		return "", Asset{}, false
	}
	p := strings.TrimPrefix(path.Clean("/"+urlPath), "/")
	var tries []string
	switch {
	case p == "":
		tries = []string{"index.html"}
	case strings.HasSuffix(urlPath, "/"):
		tries = []string{p + "/index.html"}
	default:
		tries = []string{p, p + ".html", p + "/index.html"}
	}
	for _, t := range tries {
		if a, ok := s.files[t]; ok {
			return t, a, true
		}
	}
	return "", Asset{}, false
}

// Open opens a file Find returned.
func (s *Site) Open(name string) (*os.File, error) {
	return os.Open(filepath.Join(s.dir, filepath.FromSlash(name)))
}

// dropBundle deletes a bundle no deployment uses any more, and its files.
func (p *Platform) dropBundle(sha string) {
	os.Remove(filepath.Join(p.bundlesDir, sha+".json"))
	dir := p.assetsDir(sha)
	os.RemoveAll(dir)
	p.sites.Range(func(id, s any) bool {
		if s.(*Site).dir == dir {
			p.sites.Delete(id)
		}
		return true
	})
}
