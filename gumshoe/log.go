package gumshoe

// This file defines an interface for a simple logger that is a subset of log.Logger. This is so that we can
// create a global logger instance which may be a dummy logger that doesn't do anything (for tests).
//
// NOTE(caleb): This is a pretty simple and workable approach for now. We could get fancier later with a more
// full-featured logging package (there are many of these, including https://github.com/golang/glog).

type Logger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

type nopLogger struct{}

func (nopLogger) Print(v ...interface{})                 {}
func (nopLogger) Printf(format string, v ...interface{}) {}
func (nopLogger) Println(v ...interface{})               {}
