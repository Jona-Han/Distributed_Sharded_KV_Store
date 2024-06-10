/*
Package shardctrler provides mechanisms to manage shard configurations in a distributed system.
It allows joining new groups, leaving groups, and moving shards between groups.
*/
package shardctrler

import "fmt"

// LogTopic represents different logging topics for classification.
type LogTopic int

const (
	// LogTopicClerk represents logs related to Clerk operations.
	LogTopicClerk LogTopic = iota
	// LogTopicServer represents logs related to Server operations.
	LogTopicServer
)

// TermColor is the default terminal color reset code.
const TermColor string = "\x1b[0m"

// Logger is responsible for logging messages with topic-specific formatting.
type Logger struct {
	sid int			// server ID
}

// NewLogger creates a new Logger instance with the given server ID.
func NewLogger(sId int) (*Logger, error) {
	logger := &Logger{
		sid: sId,
	}
	return logger, nil
}

// Log logs a message under the specified topic with appropriate formatting.
func (l *Logger) Log(topic LogTopic, message string) {
	topicStr, _ := l.topicToString(topic)
	color := l.sIdToColor(l.sid)

	// Center the topic string within 35 characters
	leftAlgn := fmt.Sprintf("S%d [%s]", l.sid, topicStr)

	if Debug {
		fmt.Printf("%s%-22s:%s %s\n", color, leftAlgn, "\x1b[0m", message)
	}
}

// sIdToColor returns a terminal color code based on the server ID.
func (l *Logger) sIdToColor(sId int) string {
	switch {
	case sId == 0:
		return "\x1b[31m" // Red color
	case sId == 1:
		return "\x1b[32m" // Green color
	case sId == 2:
		return "\x1b[33m" // Yellow color
	case sId == 3:
		return "\x1b[34m" // Blue color
	case sId == 4:
		return "\x1b[35m" // Magenta color
	case sId == 5:
		return "\x1b[36m" // Cyan color
	case sId == 6:
		return "\x1b[37m" // White color
	case sId == 7:
		return "\x1b[91m" // Light red color
	case sId == 8:
		return "\x1b[92m" // Light green color
	case sId == 9:
		return "\x1b[93m" // Light yellow color
	default:
		rotatingColors := []string{"\x1b[94m", "\x1b[95m", "\x1b[96m", "\x1b[97m"}
		return rotatingColors[sId%4] // Rotate between the available colors
	}

}

// topicToString converts a LogTopic to its string representation and associated color.
func (l *Logger) topicToString(topic LogTopic) (string, string) {
	switch topic {
	case LogTopicClerk:
		return "CLERK", "\x1b[34m" // Blue color
	case LogTopicServer:
		return "SERVER", "\x1b[31m" // Red color
	default:
		return "MISC", "\x1b[97m" // Bright white color
	}
}
