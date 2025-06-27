package logger

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	logTmFmtWithMS = "2006-01-02 15:04:05.000"
)

var _logger *zap.Logger
var _sugaredLogger *zap.SugaredLogger

func InitLogger(path, level string, console bool) {
	var core zapcore.Core
	if console {
		core = zapcore.NewTee(
			zapcore.NewCore(loggerEncoder(), loggerWriter(path), loggerLevel(level)),
			zapcore.NewCore(loggerEncoder(), zapcore.AddSync(os.Stdout), loggerLevel(level)),
		)
	} else {
		core = zapcore.NewCore(loggerEncoder(), loggerWriter(path), loggerLevel(level))
	}

	_logger = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(2))
	defer _logger.Sync()
	_sugaredLogger = _logger.Sugar()
}

func Logger() *zap.Logger {
	return _logger
}

func SLogger() *zap.SugaredLogger {
	return _sugaredLogger
}

func loggerLevel(level string) zapcore.Level {
	switch strings.ToLower(level) {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

func loggerEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()

	encoderConfig.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString("[" + t.Format(logTmFmtWithMS) + "]")
	}
	encoderConfig.EncodeLevel = func(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString("[" + level.CapitalString() + "]")
	}
	encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	encoderConfig.EncodeCaller = func(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString("[" + caller.TrimmedPath() + "]")
	}

	return zapcore.NewConsoleEncoder(encoderConfig)
}

func loggerWriter(path string) zapcore.WriteSyncer {
	lumberJackLogger := &lumberjack.Logger{
		Filename: path,

		// MaxSize is the maximum size in megabytes of the log file before it gets
		// rotated. It defaults to 100 megabytes.
		MaxSize: 500, // MB

		// MaxAge is the maximum number of days to retain old log files based on the
		// timestamp encoded in their filename.  Note that a day is defined as 24
		// hours and may not exactly correspond to calendar days due to daylight
		// savings, leap seconds, etc. The default is not to remove old log files
		// based on age.
		MaxAge: 7, // days

		// MaxBackups is the maximum number of old log files to retain.  The default
		// is to retain all old log files (though MaxAge may still cause them to get
		// deleted.)
		MaxBackups: 5,
	}
	return zapcore.AddSync(lumberJackLogger)
}

func writeLog(ctx context.Context, level zapcore.Level, template string, args ...any) {
	msg := fmt.Sprintf(template, args...)
	switch level {
	case zapcore.InfoLevel:
		_logger.Info(msg)
		break
	case zapcore.WarnLevel:
		_logger.Warn(msg)
		break
	case zapcore.ErrorLevel:
		_logger.Error(msg)
		break
	case zapcore.PanicLevel:
		_logger.Panic(msg)
		break
	case zapcore.FatalLevel:
		_logger.Fatal(msg)
		break
	case zapcore.DebugLevel:
		_logger.Debug(msg)
		break
	default:
	}
}

func Debug(ctx context.Context, template string, args ...any) {
	writeLog(ctx, zapcore.DebugLevel, template, args...)
}

func Info(ctx context.Context, template string, args ...any) {
	writeLog(ctx, zapcore.InfoLevel, template, args...)
}

func Warn(ctx context.Context, template string, args ...any) {
	writeLog(ctx, zapcore.WarnLevel, template, args...)
}

func Error(ctx context.Context, template string, args ...any) {
	writeLog(ctx, zapcore.ErrorLevel, template, args...)
}

func Panic(ctx context.Context, template string, args ...any) {
	writeLog(ctx, zapcore.PanicLevel, template, args...)
}

func Fatal(ctx context.Context, template string, args ...any) {
	writeLog(ctx, zapcore.FatalLevel, template, args...)
}
