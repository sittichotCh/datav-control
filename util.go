package dbcontrol

import "time"

func ToString(ptr *string) string {
	if ptr == nil {
		return ""
	}

	return *ptr
}

func ToPtrString(str string) *string {
	return &str
}

func ToPtrTime(t time.Time) *time.Time {
	return &t
}
