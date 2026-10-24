package httptransport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/application"
)

type Options struct {
	Address            string
	ShutdownTimeout    time.Duration
	Service            *application.Service
	PublicBaseURL      string
	CookieKey          []byte
	CookieSecure       bool
	TrustedProxyHeader string
	TrustedProxyHops   int
}

type Server struct {
	opts   Options
	origin string
	server *http.Server
	ready  atomic.Bool
}

func New(opts Options) (*Server, error) {
	if opts.Service == nil || len(opts.CookieKey) == 0 || opts.Address == "" || opts.ShutdownTimeout <= 0 {
		return nil, errors.New("edge server requires an address, shutdown timeout, service and cookie key")
	}
	base, err := url.Parse(opts.PublicBaseURL)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("edge server requires an absolute public base URL")
	}
	if opts.TrustedProxyHops < 1 {
		opts.TrustedProxyHops = 1
	}
	s := &Server{opts: opts, origin: base.Scheme + "://" + base.Host}
	s.server = &http.Server{
		Addr:              opts.Address,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return s, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /ready", s.readiness)
	mux.HandleFunc("POST /v1/payments", s.createPayment)
	mux.HandleFunc("GET /v1/payments/{payment_id}", s.getPayment)
	mux.HandleFunc("POST /v1/checkout/sessions/{session_id}/attempts", s.createAttempt)
	mux.HandleFunc("POST /v1/payment-methods", s.createPaymentMethod)
	mux.HandleFunc("GET /checkout/{session_id}", s.checkoutPage)
	mux.HandleFunc("POST /checkout/{session_id}/pay", s.checkoutPay)
	mux.HandleFunc("GET /checkout/{session_id}/status", s.checkoutStatus)
	return observe.HTTPServer(mux, s.withCorrelation(s.withRecovery(mux)))
}

func (s *Server) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.opts.Address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.opts.Address, err)
	}
	return s.Serve(ctx, listener)
}

func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	errCh := make(chan error, 1)
	s.ready.Store(true)
	go func() { errCh <- s.server.Serve(listener) }()

	select {
	case err := <-errCh:
		s.ready.Store(false)
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		s.ready.Store(false)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.opts.ShutdownTimeout)
	defer cancel()
	if err := s.server.Shutdown(shutdownCtx); err != nil {
		closeErr := s.server.Close()
		return errors.Join(fmt.Errorf("shutdown edge service: %w", err), closeErr)
	}
	err := <-errCh
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readiness(w http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
