package raft

import (
	"log"
)

type LogLvl int

const (
	_trace LogLvl = 0
	_debug = 1
	_info = 2
	_warn = 3
	_error = 4
	_no_log = 5
)

// Debugging
const CurrentLogLvl = _no_log

func DPrintf(format string, a ...interface{}) (n int, err error) {
	print(_debug, format, a...)
	return
}

func print(lvl LogLvl, format string, a ...interface{}) (n int, err error) {
	if CurrentLogLvl <= lvl {
		log.Printf(format, a...)
	}
	return
}

func Trace(format string, a ...interface{}) {
	print(_trace, "[TRACE] " + format, a...)
}

func Debug(format string, a ...interface{}) {
	print(_debug, "[DEBUG] " + format, a...)
}

func Info(format string, a ...interface{}) {
	print(_info, "[INFO ] " + format, a...)
}

func Warn(format string, a ...interface{}) {
	print(_warn, "[WARN ] " + format, a...)
}

func Error(format string, a ...interface{}) {
	print(_error, "[ERROR] " + format, a...)
}