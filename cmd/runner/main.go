// Command runner runs the Zeroko runner: the gateway, the API, the control
// socket for service bindings, and one workerd process per running service.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/zeroko-io/runner/internal/config"
	"github.com/zeroko-io/runner/internal/platform"
	"github.com/zeroko-io/runner/internal/server"
	"github.com/zeroko-io/runner/internal/store"
)

var version = "dev"

func main() {
	lines := platform.NewLines(5000)
	log := slog.New(slog.NewTextHandler(io.MultiWriter(os.Stderr, lines), &slog.HandlerOptions{Level: slog.LevelInfo}))
	err := asUser()
	if err == nil {
		err = run(log, lines)
	}
	if errors.Is(err, errRestart) {
		// In place, as the same process: the container does not notice.
		err = syscall.Exec("/proc/self/exe", os.Args, os.Environ())
	}
	if err != nil {
		log.Error("runner stopped", "err", err)
		os.Exit(1)
	}
}

var errRestart = errors.New("restart")

func run(log *slog.Logger, lines *platform.Lines) error {
	// The root token is for the app that manages the runner; everything else
	// is set through the API.
	token := strings.TrimSpace(os.Getenv("ZEROKO_TOKEN"))
	if len(token) < 32 {
		return errors.New("ZEROKO_TOKEN is required: the root API token, 32 characters or more (openssl rand -hex 32)")
	}
	if err := os.MkdirAll(config.DataDir, 0o700); err != nil {
		return err
	}
	// One runner per data directory: a second one would remove the first's
	// sockets.
	lock, err := os.OpenFile(filepath.Join(config.DataDir, ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return errors.New("another runner is running on " + config.DataDir)
	}
	if restored, err := platform.ApplyRestore(); err != nil {
		return fmt.Errorf("restore the backup: %w", err)
	} else if restored {
		log.Info("backup restored")
	}
	st, err := store.Open(filepath.Join(config.DataDir, "runner.db"))
	if err != nil {
		return err
	}
	defer st.Close()
	cfg, err := st.Settings(context.Background())
	if err != nil {
		return fmt.Errorf("read the settings: %w", err)
	}

	p, err := platform.New(cfg, st, log)
	if err != nil {
		return err
	}
	p.Version, p.Lines = version, lines
	defer p.Close()

	srv := server.New(p, token, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	os.Remove(p.ControlSock())
	ctl, err := net.Listen("unix", p.ControlSock())
	if err != nil {
		return err
	}
	controlSrv := &http.Server{Handler: srv.Control(), ReadHeaderTimeout: 10 * time.Second}
	go controlSrv.Serve(ctl)
	defer controlSrv.Close()

	if err := p.Start(ctx); err != nil {
		return err
	}

	listeners := []*http.Server{
		{Addr: config.GatewayAddr, Handler: srv.Gateway()},
		{Addr: config.APIAddr, Handler: srv.API()},
	}
	errc := make(chan error, len(listeners))
	for _, l := range listeners {
		l.ReadHeaderTimeout, l.IdleTimeout, l.MaxHeaderBytes = 10*time.Second, 120*time.Second, 64<<10
		go func() { errc <- l.ListenAndServe() }()
	}
	log.Info("runner started", "version", version, "gateway", config.GatewayAddr, "api", config.APIAddr, "domain", cfg.Domain)

	restart := false
	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
	case <-p.Restarting():
		restart = true
	}
	log.Info("shutting down")
	// Realtime connections and log long-polls would hold Shutdown open: close
	// them, give requests in flight a few seconds, then close the rest.
	p.Hub.CloseAll()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, l := range listeners {
		if l.Shutdown(shutdownCtx) != nil {
			l.Close()
		}
	}
	if restart {
		return errRestart
	}
	return nil
}
