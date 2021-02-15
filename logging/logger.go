package logging

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap/zapcore"

	"go.uber.org/zap"
)

// NewLogger creates a preconfigured global logger and configures the global zap logger
func NewLogger(cfg Config, metricsNamespace string) *zap.Logger {
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	// Parse log level text to zap.LogLevel. Error check isn't required because the input is already validated.
	level := zap.NewAtomicLevel()
	_ = level.UnmarshalText([]byte(cfg.Level))

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.Lock(os.Stdout),
		level,
	)
	core = zapcore.RegisterHooks(core, prometheusHook(metricsNamespace))
	logger := zap.New(core)
	zap.ReplaceGlobals(logger)

	return logger
}

// prometheusHook is a hook for the zap library which exposes Prometheus counters for various log levels.
func prometheusHook(metricsNamespace string) func(zapcore.Entry) error {
	messageCounterVec := promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "log_messages_total",
		Help:      "Total number of log messages by log level emitted by KMinion.",
	}, []string{"level"})

	// Initialize counters for all supported log levels so that they expose 0 for each level on startup
	supportedLevels := []zapcore.Level{
		zapcore.DebugLevel,
		zapcore.InfoLevel,
		zapcore.WarnLevel,
		zapcore.ErrorLevel,
		zapcore.FatalLevel,
		zapcore.PanicLevel,
	}
	for _, level := range supportedLevels {
		messageCounterVec.WithLabelValues(level.String())
	}

	return func(entry zapcore.Entry) error {
		messageCounterVec.WithLabelValues(entry.Level.String()).Inc()
		return nil
	}
}
