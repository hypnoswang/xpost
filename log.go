package xpost

import ()

func logDebug(args ...interface{}) {
	logger.Debug(args...)
}

func logDebugf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

func logDebugln(args ...interface{}) {
	logger.Debugln(args...)
}

func logInfo(args ...interface{}) {
	logger.Info(args...)
}

func logInfof(format string, args ...interface{}) {
	logger.Infof(format, args...)
}

func logInfoln(args ...interface{}) {
	logger.Infoln(args...)
}

func logWarn(args ...interface{}) {
	logger.Warn(args...)
}

func logWarnf(format string, args ...interface{}) {
	logger.Warnf(format, args...)
}

func logWarnln(args ...interface{}) {
	logger.Warnln(args...)
}

func logError(args ...interface{}) {
	logger.Error(args...)
}

func logErrorf(format string, args ...interface{}) {
	logger.Errorf(format, args...)
}

func logErrorln(args ...interface{}) {
	logger.Errorln(args...)
}

func logFatal(args ...interface{}) {
	logger.Fatal(args...)
}

func logFatalf(format string, args ...interface{}) {
	logger.Fatalf(format, args...)
}

func logFatalln(args ...interface{}) {
	logger.Panicln(args...)
}

func logPanic(args ...interface{}) {
	logger.Panic(args...)
}

func logPanicf(format string, args ...interface{}) {
	logger.Panicf(format, args...)
}

func logPanicln(args ...interface{}) {
	logger.Panicln(args...)
}
