package qstat

import (
	"testing"
)

func TestTaskIDRange(t *testing.T) {
	tests := []struct {
		in       string
		expected TaskIDRange
		valid    bool
	}{
		{"", TaskIDRange{1, 1, 1}, true},
		{"1", TaskIDRange{1, 1, 1}, true},
		{"16", TaskIDRange{16, 16, 1}, true},
		{"1-10", TaskIDRange{1, 10, 1}, true},
		{"1-10:3", TaskIDRange{1, 10, 3}, true},
		{"6-8", TaskIDRange{6, 8, 1}, true},
		{"1-10:3:4", TaskIDRange{}, false},
		{"1--10", TaskIDRange{}, false},
	}

	for i, test := range tests {
		out, err := NewTaskIDRange(test.in)
		if err != nil && test.valid {
			t.Errorf("%d: error %s for %v", i, err, test.in)
		} else if err == nil && !test.valid {
			t.Errorf("%d: expected error but didn't get one for %v", i, test.in)
		} else {
			if out != test.expected {
				t.Errorf("%d: got %v but expected %v")
			}
		}
	}
}

func TestNumTasks(t *testing.T) {
	tests := []struct {
		tRange   TaskIDRange
		NumTasks int
	}{
		{TaskIDRange{1, 1, 1}, 1},
		{TaskIDRange{1, 10, 1}, 10},
		{TaskIDRange{1, 10, 2}, 5},
		{TaskIDRange{1, 10, 3}, 4},
		{TaskIDRange{6, 8, 1}, 3},
	}

	for i, test := range tests {
		n := test.tRange.NumTasks()
		if n != test.NumTasks {
			t.Errorf("%d: got %d, expected %d", i, n, test.NumTasks)
		}
	}
}
