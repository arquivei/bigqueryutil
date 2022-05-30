package bigqueryutil

import "time"

// TimeRange represents a time with a beginning and an end
type TimeRange struct {
	From time.Time
	To   time.Time
}
