package deploy

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule is a parsed five-field cron expression (minute, hour, day of
// month, month, day of week), evaluated in UTC.
type Schedule struct {
	fields         [5]uint64
	domAny, dowAny bool
}

var cronRanges = [5][2]int{{0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 7}}

func ParseCron(expr string) (*Schedule, error) {
	parts := strings.Fields(expr)
	if len(parts) != 5 {
		return nil, errors.New("want five fields: minute hour day-of-month month day-of-week")
	}
	var s Schedule
	for i, p := range parts {
		bits, err := parseField(p, cronRanges[i][0], cronRanges[i][1])
		if err != nil {
			return nil, fmt.Errorf("field %d (%s): %w", i+1, p, err)
		}
		s.fields[i] = bits
	}
	if s.fields[4]&(1<<7) != 0 { // 7 is Sunday too
		s.fields[4] |= 1
	}
	s.domAny, s.dowAny = parts[2] == "*", parts[4] == "*"
	return &s, nil
}

func parseField(f string, lo, hi int) (uint64, error) {
	var bits uint64
	for _, item := range strings.Split(f, ",") {
		step := 1
		if base, st, ok := strings.Cut(item, "/"); ok {
			n, err := strconv.Atoi(st)
			if err != nil || n < 1 {
				return 0, errors.New("invalid step")
			}
			item, step = base, n
		}
		a, b := lo, hi
		if item != "*" {
			x, y, isRange := strings.Cut(item, "-")
			var err error
			if a, err = strconv.Atoi(x); err != nil {
				return 0, errors.New("invalid number")
			}
			b = a
			if isRange {
				if b, err = strconv.Atoi(y); err != nil {
					return 0, errors.New("invalid number")
				}
			} else if step > 1 {
				b = hi
			}
		}
		if a < lo || b > hi || a > b {
			return 0, fmt.Errorf("out of range %d-%d", lo, hi)
		}
		for v := a; v <= b; v += step {
			bits |= 1 << v
		}
	}
	return bits, nil
}

// Matches reports whether the schedule fires in the minute of t (UTC).
func (s *Schedule) Matches(t time.Time) bool {
	t = t.UTC()
	has := func(i, v int) bool { return s.fields[i]&(1<<v) != 0 }
	if !has(0, t.Minute()) || !has(1, t.Hour()) || !has(3, int(t.Month())) {
		return false
	}
	dom, dow := has(2, t.Day()), has(4, int(t.Weekday()))
	switch {
	case s.domAny && s.dowAny:
		return true
	case s.domAny:
		return dow
	case s.dowAny:
		return dom
	}
	return dom || dow // as in Vixie cron, either day field may match
}
