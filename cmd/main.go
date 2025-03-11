package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	hspb "sendingemail/genproto/go/http/v1"
	"sendingemail/internal/sender"

	"github.com/labstack/echo/v4"
	stdmw "github.com/labstack/echo/v4/middleware"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	_ "github.com/denisenkom/go-mssqldb"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("Failed to run the server: %s", err)
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

type Album struct {
	ID     int
	Title  string
	Artist string
	Price  int
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	zlog, err := newLogger()
	if err != nil {
		return err
	}
	defer zlog.Sync()
	zap.ReplaceGlobals(zlog)

	db, err := sql.Open(
		"sqlserver",
		fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s&TrustServerCertificate=true",
			os.Getenv("DB_USER"),
			os.Getenv("DB_PASSWORD"),
			os.Getenv("DB_HOST"),
			os.Getenv("DB_PORT"),
			os.Getenv("DB_NAME"),
		),
	)
	if err != nil {
		return fmt.Errorf("Failed to create db connection: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("Failed to ping DB: %w", err)
	}

	db.SetConnMaxIdleTime(5)
	db.SetMaxOpenConns(30)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(10 * time.Minute)

	senderSvc, err := sender.NewService(ctx, db, zlog)
	if err != nil {
		return fmt.Errorf("Failed to create sender service: %w", err)
	}

	scheduled := gocron.NewScheduler(time.Local)
	scheduled.Every(10).Seconds().Do(func() {
		zlog.Info("Starting cron job to send emails")
		senderSvc.Send(ctx)
	})
	scheduled.StartAsync()

	e := echo.New()
	e.HideBanner = true
	e.HTTPErrorHandler = httpErr
	e.Use(stdmws()...)
	e.GET("/v1/healthz", func(c echo.Context) error {
		ctx, cancel := context.WithTimeout(c.Request().Context(), 5*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			return err
		}
		return c.JSON(http.StatusOK, echo.Map{
			"code":    http.StatusOK,
			"status":  "OK",
			"message": "Available!",
		})
	})

	errChan := make(chan error, 1)
	go func() {
		errChan <- e.Start(fmt.Sprintf(":%s", getEnv("PORT", "8089")))
	}()

	ctx, cancel = signal.NotifyContext(ctx, os.Interrupt, os.Kill, syscall.SIGTERM)
	defer cancel()

	select {
	case <-ctx.Done():
		zlog.Info("Shutting down the server...")
		if err := e.Shutdown(ctx); err != nil {
			return fmt.Errorf("Failed to shutdown the server: %w", err)
		}

		zlog.Info("The server shut down gracefully")

	case err := <-errChan:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
	}

	return nil
}

func newLogger() (*zap.Logger, error) {
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "timestamp",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "message",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	config := zap.Config{
		Level:            zap.NewAtomicLevelAt(zap.DebugLevel),
		Development:      false,
		Encoding:         "console",
		EncoderConfig:    encoderConfig,
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	zlog, err := config.Build()
	if err != nil {
		return nil, fmt.Errorf("Failed to build zap log: %w", err)
	}

	return zlog, nil
}

func stdmws() []echo.MiddlewareFunc {
	return []echo.MiddlewareFunc{
		stdmw.RemoveTrailingSlash(),
		stdmw.Recover(),
		stdmw.CORSWithConfig(stdmw.CORSConfig{
			AllowOriginFunc: func(origin string) (bool, error) {
				return true, nil
			},
			AllowMethods: []string{
				http.MethodHead,
				http.MethodGet,
				http.MethodPost,
				http.MethodPut,
				http.MethodPatch,
				http.MethodDelete,
				http.MethodOptions,
			},
			AllowCredentials: true,
			MaxAge:           86400,
		}),
		stdmw.RateLimiter(stdmw.NewRateLimiterMemoryStore(10)),
		stdmw.Secure(),
	}
}

func httpErr(err error, c echo.Context) {
	if s, ok := status.FromError(err); ok {
		he := httpStatusPbFromRPC(s)
		jsonb, _ := protojson.Marshal(he)
		c.JSONBlob(int(he.Error.Code), jsonb)
		return
	}

	if he, ok := err.(*echo.HTTPError); ok {
		var s *status.Status
		switch he.Code {
		case http.StatusNotFound:
			s = status.New(codes.NotFound, "Not found!")

		case http.StatusTooManyRequests:
			s = status.New(codes.ResourceExhausted, "Too many requests.")

		default:
			s = status.New(codes.Unknown, "Unknown error!")
		}

		hbp := httpStatusPbFromRPC(s)
		jsonb, _ := protojson.Marshal(hbp)
		c.JSONBlob(int(hbp.Error.Code), jsonb)
		return
	}

	c.JSON(http.StatusInternalServerError, echo.Map{
		"code":    500,
		"status":  "INTERNAL_ERROR",
		"message": "An internal error occurred",
	})
}

func httpStatusPbFromRPC(s *status.Status) *hspb.Error {
	return &hspb.Error{
		Error: &hspb.Status{
			Code:    int32(runtime.HTTPStatusFromCode(s.Code())),
			Message: s.Message(),
			Status:  code.Code(s.Code()),
			Details: s.Proto().GetDetails(),
		},
	}
}
