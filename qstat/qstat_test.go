package qstat

import (
	"encoding/xml"
	"reflect"
	"testing"
)

const queueInfo = `<?xml version='1.0'?>
<job_info  xmlns:xsd="http://gridengine.sunsource.net/source/browse/*checkout*/gridengine/source/dist/util/resources/schemas/qstat/qstat.xsd?revision=1.11">
  <queue_info>
    <job_list state="running">
      <JB_job_number>3064076</JB_job_number>
      <JAT_prio>0.67712</JAT_prio>
      <JAT_ntix>1.00000</JAT_ntix>
      <JB_nurg>0.00064</JB_nurg>
      <JB_urg>527</JB_urg>
      <JB_rrcontr>512</JB_rrcontr>
      <JB_wtcontr>15</JB_wtcontr>
      <JB_dlcontr>0</JB_dlcontr>
      <JB_nppri>0.25586</JB_nppri>
      <JB_priority>-500</JB_priority>
      <JB_name>QRLOGIN</JB_name>
      <JB_owner>bob</JB_owner>
      <JB_project>some_project</JB_project>
      <JB_department>defaultdepartment</JB_department>
      <state>r</state>
      <JAT_start_time>2012-11-01T13:06:41</JAT_start_time>
      <cpu_usage>0.00000</cpu_usage>
      <mem_usage>0.00000</mem_usage>
      <io_usage>0.00000</io_usage>
      <tickets>666</tickets>
      <JB_override_tickets>0</JB_override_tickets>
      <JB_jobshare>0</JB_jobshare>
      <otickets>0</otickets>
      <ftickets>666</ftickets>
      <stickets>0</stickets>
      <JAT_share>0.16667</JAT_share>
      <queue_name>interactive.q@cluster</queue_name>
      <slots>1</slots>
    </job_list>
  </queue_info>
  <job_info>
    <job_list state="pending">
      <JB_job_number>3050948</JB_job_number>
      <JAT_prio>0.70234</JAT_prio>
      <JAT_ntix>1.00000</JAT_ntix>
      <JB_nurg>0.00064</JB_nurg>
      <JB_urg>527</JB_urg>
      <JB_rrcontr>512</JB_rrcontr>
      <JB_wtcontr>15</JB_wtcontr>
      <JB_dlcontr>0</JB_dlcontr>
      <JB_nppri>0.00000</JB_nppri>
      <JB_priority>-500</JB_priority>
      <JB_name>Something</JB_name>
      <JB_owner>john</JB_owner>
      <JB_project>some_other_project</JB_project>
      <JB_department>defaultdepartment</JB_department>
      <state>Eqw</state>
      <JB_submission_time>2012-10-28T09:47:07</JB_submission_time>
      <tickets>500</tickets>
      <JB_override_tickets>0</JB_override_tickets>
      <JB_jobshare>0</JB_jobshare>
      <otickets>0</otickets>
      <ftickets>500</ftickets>
      <stickets>0</stickets>
      <JAT_share>0.12500</JAT_share>
      <queue_name></queue_name>
      <slots>1</slots>
    </job_list>
  </job_info>
</job_info>
`

// Test the QueueInfo struct
func TestQueueInfo(t *testing.T) {
	r := QueueInfo{}
	err := xml.Unmarshal([]byte(queueInfo), &r)
	if err != nil {
		t.Errorf("Unmarshal failed: %s", err)
	}
	if len(r.QueuedJobs) != 1 {
		t.Errorf("Wrong number of queued jobs: %d", len(r.QueuedJobs))
	}
	if len(r.PendingJobs) != 1 {
		t.Errorf("Wrong number of pending jobs: %d", len(r.PendingJobs))
	}

	qj := r.QueuedJobs[0]
	queuedExpected := QueueJob{
		JobNumber:            3064076,
		NormalizedPriority:   0.67712,
		NormalizedUrgency:    0.00064,
		NormalizedTickets:    1,
		ResourceContribution: 512,
		WaitTimeContribution: 15,
		DeadlineContribution: 0,
		POSIXPriority:        -500,
		Name:                 "QRLOGIN",
		Owner:                "bob",
		Project:              "some_project",
		Department:           "defaultdepartment",
		State:                "r",
		StartTime:            "2012-11-01T13:06:41",
		SubmissionTime:       "",
		CPUUsage:             0.0,
		MemUsage:             0.0,
		IOUsage:              0.0,
		Tickets:              666,
		OverrideTickets:      0,
		FairshareTickets:     666,
		ShareTreeTickets:     0,
		QueueName:            "interactive.q@cluster",
		Slots:                1,
	}
	if !reflect.DeepEqual(qj, queuedExpected) {
		t.Errorf("Queued job got %v, expected %v", qj, queuedExpected)
	}
	if tasks := qj.NumTasks(); tasks != 1 {
		t.Errorf("Expected 1 task, got %d", tasks)
	}
	if !(!qj.DeletionState() && !qj.ErrorState() && !qj.HoldState() && qj.RunningState() && !qj.RestartedState() &&
		!qj.SuspendedState() && !qj.QueueSuspendedState() && !qj.TransferringState() && !qj.ThresholdState() &&
		!qj.WaitingState() && !qj.QueuedState()) {
		t.Errorf("Invalid state combinations for queued job")
	}

	pj := r.PendingJobs[0]
	pendingExpected := QueueJob{
		JobNumber:            3050948,
		NormalizedPriority:   0.70234,
		NormalizedUrgency:    0.00064,
		NormalizedTickets:    1,
		POSIXPriority:        -500,
		ResourceContribution: 512,
		WaitTimeContribution: 15,
		DeadlineContribution: 0,
		Name:                 "Something",
		Owner:                "john",
		Project:              "some_other_project",
		Department:           "defaultdepartment",
		State:                "Eqw",
		StartTime:            "",
		SubmissionTime:       "2012-10-28T09:47:07",
		CPUUsage:             0.0,
		MemUsage:             0.0,
		IOUsage:              0.0,
		Tickets:              500,
		OverrideTickets:      0,
		FairshareTickets:     500,
		ShareTreeTickets:     0,
		QueueName:            "",
		Slots:                1,
	}
	if !reflect.DeepEqual(pj, pendingExpected) {
		t.Errorf("Queued job got %v, expected %v", pj, pendingExpected)
	}
	if tasks := qj.NumTasks(); tasks != 1 {
		t.Errorf("Expected 1 task, got %d", tasks)
	}
	if !(!pj.DeletionState() && pj.ErrorState() && !pj.HoldState() && !pj.RunningState() && !pj.RestartedState() &&
		!pj.SuspendedState() && !pj.QueueSuspendedState() && !pj.TransferringState() && !pj.ThresholdState() &&
		pj.WaitingState() && pj.QueuedState()) {
		t.Errorf("Invalid state combinations for queued job")
	}
}

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
				t.Errorf("%d: got %v but expected %v", i, out, test.expected)
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
