// Copyright 2012 Kamil Kisiel. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package qstat provides an interface to GridEngine's job and queue status facilities
package qstat

import (
	"encoding/xml"
	"fmt"
	"github.com/kisielk/gorge/util"
	"math"
	"os/exec"
	"path"
	"strconv"
	"strings"
)

// Resource represents a GridEngine resource request
// See man 5 sge_complex for a more detailed description of the fields
type Resource struct {
	Name        string  `json:"name" xml:"CE_name"`                // The name of the complex resource
	ValType     int     `json:"valType" xml:"CE_valtype"`          // The type of value
	StringVal   string  `json:"stringVal" xml:"CE_stringval"`      // The value as a string
	DoubleVal   float64 `json:"doubleVal" xml:"CE_doubleval"`      // The value as a double
	RelOp       int     `json:"relOp" xml:"CE_relop"`              // The relation operator used to compare the value
	Consumable  bool    `json:"consumable" xml:"CE_consumable"`    // True if the resource is a consumable resourece
	Dominant    bool    `json:"dominant" xml:"CE_dominant"`        // ?
	PJDoubleVal float64 `json:"pjDoubleVal" xml:"CE_pj_doubleval"` // ?
	PJDominant  bool    `json:"pjDominant" xml:"CE_pj_dominant"`   // ?
	Requestable bool    `json:"requestable" xml:"CE_requestable"`  // True if the resource is a requestable resource
	Tagged      bool    `json:"tagged" xml:"CE_tagged"`            // ?
}

// MailAddress represents an email address
type MailAddress struct {
	User string `json:"user" xml:"MR_user"`
	Host string `json:"host" xml:"MR_host"`
}

// String implements the Stringer interface
func (a MailAddress) String() string {
	return a.User + "@" + a.Host
}

// EnvVar represents a job environment variable
type EnvVar struct {
	Variable string `json:"variable" xml:"VA_variable"` // The name of the variable
	Value    string `json:"value" xml:"VA_value"`       // The value of the variable
}

type PathList struct {
	Path        string `json:"path" xml:"PN_path"`
	Host        string `json:"host" xml:"PN_host"`
	FileHost    string `json:"fileHost" xml:"PN_file_host"`
	FileStaging bool   `json:"fileStaging" xml:"PN_file_staging"`
}

// TaskIDRange represents a range of job array task identifiers
type TaskIDRange struct {
	Min  int `json:"min" xml:"RN_min"`   // The minimum task ID
	Max  int `json:"max" xml:"RN_max"`   // The maximum task ID
	Step int `json:"step" xml:"RN_step"` // The ID step size between tasks
}

// NumTasks returns the number of tasks in a range
func (r TaskIDRange) NumTasks() int {
	min := float64(r.Min)
	max := float64(r.Max)
	step := float64(r.Step)
	return int(math.Ceil((max - min + 1) / step))
}

// NewTaskIDRange initializes a TaskIDRange from a string range expression.
// The range expression is in one of the forms:
//
//		* an empty string
//		* n
//		* n-m
//		* n-m:s
//
// where n is the first task number, m is the last task number, and s is the
// step size. An empty string will return a range equivalent to the string "1".
func NewTaskIDRange(s string) (TaskIDRange, error) {
	// Blank string is assumed to be a non-array
	if s == "" {
		return TaskIDRange{1, 1, 1}, nil
	}

	var min, max, step int64 = 1, 1, 1
	var err error

	parts := strings.Split(s, "-")

	min, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return TaskIDRange{}, fmt.Errorf("could not parse: invalid min (%s)", parts[0])
	}

	// If there's no other parts, max == min
	max = min

	if len(parts) > 2 {
		return TaskIDRange{}, fmt.Errorf("could not parse: too many elements in parts split")
	} else if len(parts) == 2 {
		tail := strings.Split(parts[1], ":")
		if len(tail) > 2 {
			return TaskIDRange{}, fmt.Errorf("could not parse: too many elements in step split")
		} else if len(tail) == 2 {
			step, err = strconv.ParseInt(tail[1], 10, 64)
			if err != nil {
				return TaskIDRange{}, fmt.Errorf("could not parse: invalid step (%s)", tail[1])
			}
		}
		max, err = strconv.ParseInt(tail[0], 10, 64)
		if err != nil {
			return TaskIDRange{}, fmt.Errorf("could not parse: invalid max (%s)", tail[0])
		}
	}

	return TaskIDRange{int(min), int(max), int(step)}, nil
}

// ParseTaskIDRanges creates a slice of TaskIDRange based on the string s
func ParseTaskIDRanges(s string) ([]TaskIDRange, error) {
	rangeStrings := strings.Split(s, ",")
	ranges := []TaskIDRange{}
	for _, r := range rangeStrings {
		IDRange, err := NewTaskIDRange(r)
		if err != nil {
			return []TaskIDRange{}, err
		}
		ranges = append(ranges, IDRange)
	}
	return ranges, nil
}

type JATMessage struct {
	Type    int    `json:"type" xml:"QIM_type"`
	Message string `json:"message" xml:"QIM_message"`
}

type Messages struct {
	Messages       []SMEMessage `json:"messages" xml:"SME_message_list>element"`
	GlobalMessages []SMEMessage `json:"globalMessages" xml:"SME_global_message_list>element"`
}

type SMEMessage struct {
	JobNumbers []int  `json:"jobNumbers" xml:"MES_job_number_list>ulong_sublist>ULNG_value"`
	Number     int    `json:"number" xml:"MES_message_number"`
	Message    string `json:"message" xml:"MES_message"`
}

type Task struct {
	Status      int          `json:"status" xml:"JAT_status"`
	TaskNumber  int          `json:"taskNumber" xml:"JAT_task_number"`
	MessageList []JATMessage `json:"messageList" xml:"JAT_message_list>ulong_sublist"`
}

type JobInfo struct {
	JobNumber               int           `json:"jobNumber" xml:"JB_job_number"`
	AdvanceReservation      int           `json:"advanceReservation" xml:"JB_ar"`
	ExecFile                string        `json:"execFile" xml:"JB_exec_file"`
	SubmissionTime          int           `json:"submissionTime" xml:"JB_submission_time"`
	Owner                   string        `json:"owner" xml:"JB_owner"`
	Uid                     int           `json:"uid" xml:"JB_uid"`
	Group                   string        `json:"group" xml:"JB_group"`
	Gid                     int           `json:"gid" xml:"JB_gid"`
	Account                 string        `json:"account" xml:"JB_account"`
	MergeStdErr             bool          `json:"mergeStdErr" xml:"JB_merge_stderr"`
	MailList                []MailAddress `json:"mailList" xml:"JB_mail_list>element"`
	Project                 string        `json:"project" xml:"JB_project"`
	Notify                  bool          `json:"notify" xml:"JB_notify"`
	JobName                 string        `json:"jobName" xml:"JB_job_name"`
	StdoutPathList          []PathList    `json:"stdoutPathList" xml:"JB_stdout_path_list>path_list"`
	AltStdoutPathList       []PathList    `json:"altStdoutPathList" xml:"JB_stdout_path_list>stdout_path_list"` // Alternate stdout path list
	JobShare                int           `json:"jobShare" xml:"JB_jobshare"`
	QstatHardResourceList   []Resource    `json:"qstatHardResourceList" xml:"JB_hard_resource_list>qstat_l_requests"` // One type of hard resource list qstat has. Use HardResourceRequest() to get the full list.
	ElementHardResourceList []Resource    `json:"elementHardResourceList" xml:"JB_hard_resource_list>element"`        // Another type of hard resource list. Use HardResourceRequest() to get the full list.
	EnvList                 []EnvVar      `json:"envList" xml:"JB_env_list>job_sublist"`
	JobArgs                 []string      `json:"jobArgs" xml:"JB_job_args>element>ST_name"`
	ScriptFile              string        `json:"scriptFile" xml:"JB_script_file"`
	JobArrayTasks           []Task        `json:"jobArrayTasks" xml:"JB_ja_tasks>ulong_sublist"`
	Cwd                     string        `json:"cwd" xml:"JB_cwd"`
	StderrPathList          []PathList    `json:"stderrPathList" xml:"JB_stderr_path_list>path_list"`
	AltStderrPathList       []PathList    `json:"altStderrPathList" xml:"JB_stderr_path_list>stderr_path_list"` // Alternate stderr path list
	JIDRequestList          []int         `json:"jobIdRequestList" xml:"JB_jid_request_list>element>JRE_job_name"`
	JIDSuccessorList        []int         `json:"jobIdSuccessorList" xml:"JB_jid_successor_list>ulong_sublist>JRE_job_number"`
	Deadline                bool          `json:"deadline" xml:"JB_deadline"`
	ExecutionTime           int           `json:"executionTime" xml:"JB_execution_time"`
	CheckpointAttr          int           `json:"checkpointAttr" xml:"JB_checkpoint_attr"`
	CheckpointInterval      int           `json:"checkpointInterval" xml:"JB_checkpoint_interval"`
	Reserve                 bool          `json:"reserve" xml:"JB_reserve"`
	MailOptions             int           `json:"mailOptions" xml:"JB_mail_options"`
	Priority                int           `json:"priority" xml:"JB_priority"`
	Restart                 int           `json:"restart" xml:"JB_restart"`
	Verify                  bool          `json:"verify" xml:"JB_verify"`
	ScriptSize              int           `json:"scriptSize" xml:"JB_script_size"`
	VerifySuitableQueues    bool          `json:"verifySuitableQueues" xml:"JB_verify_suitable_queues"`
	SoftWallClockGMT        int           `json:"softWallClockGmt" xml:"JB_soft_wallclock_gmt"`
	HardWallClockGMT        int           `json:"hardWallClockGmt" xml:"JB_hard_wallclock_gmt"`
	OverrideTickets         int           `json:"overrideTickets" xml:"JB_override_tickets"`
	Version                 int           `json:"version" xml:"JB_version"`
	JobArray                TaskIDRange   `json:"jobArray" xml:"JB_ja_structure>task_id_range"`
	Type                    int           `json:"type" xml:"JB_type"`
}

// HardResourceList returns the complete list of the hard resource requests made by the job
func (i JobInfo) HardResourceRequest() []Resource {
	resources := make([]Resource, 0, len(i.QstatHardResourceList)+len(i.ElementHardResourceList))
	for _, r := range i.QstatHardResourceList {
		resources = append(resources, r)
	}
	for _, r := range i.ElementHardResourceList {
		resources = append(resources, r)
	}
	return resources
}

// NumTasks returns the number of tasks in a JobInfo
func (i JobInfo) NumTasks() int {
	return i.JobArray.NumTasks()
}

// DetailedJobInfo represents the job information returned by qstat -j
type DetailedJobInfo struct {
	Jobs     []JobInfo `xml:"djob_info>element"`
	Messages Messages  `xml:"messages>element"`
}

// QueueJob represents data about one job in the queue overview
type QueueJob struct {
	JobNumber            int     `json:"jobNumber" xml:"JB_job_number"`           // Unique job number
	POSIXPriority        int     `json:"posixPriority" xml:"JB_priority"`         //  Relative importance due to Posix priority in the range between 0.0 and 1.0
	NormalizedUrgency    float64 `json:"normalizedUrgency" xml:"JB_nurg"`         // Relative importance due to static urgency in the range between 0.0 and 1.0
	NormalizedPriority   float64 `json:"normalizedPriority" xml:"JAT_prio"`       // The GE priority derived from weighted normalized tickets and weighted normalized static urgency
	NormalizedTickets    float64 `json:"normalizedTickets" xml:"JAT_ntix"`        //  Relative importance due to JAT_tix amount in the range between 0.0 and 1.0.
	ResourceContribution float64 `json:"resourceContribution" xml:"JB_rrcontr"`   //  Combined contribution to static urgency from all resources.
	DeadlineContribution float64 `json:"deadlineContribution" xml:"JB_dlcontr"`   // Contribution to static urgency from job deadline.
	WaitTimeContribution float64 `json:"waitTimeContribution" xml:"JB_wtcontr"`   // Contribution to static urgency from waiting time.
	Name                 string  `json:"name" xml:"JB_name"`                      // Job name
	Owner                string  `json:"owner" xml:"JB_owner"`                    // Owner of the job
	Project              string  `json:"project" xml:"JB_project"`                // Project name
	Department           string  `json:"department" xml:"JB_department"`          // Department name
	State                string  `json:"state" xml:"state"`                       // State string
	StartTime            string  `json:"startTime" xml:"JAT_start_time"`          // Task start time
	SubmissionTime       string  `json:"submissionTime" xml:"JB_submission_time"` // Time the job was submitted
	CPUUsage             float64 `json:"cpuUsage" xml:"cpu_usage"`                // CPU usage in seconds
	MemUsage             float64 `json:"memUsage" xml:"mem_usage"`                // Memory usage in MB * seconds
	IOUsage              float64 `json:"ioUsage" xml:"io_usage"`                  // IO usage in MB
	Tickets              int     `json:"tickets" xml:"tickets"`                   // Number of assigned tickets
	OverrideTickets      int     `json:"overrideTickets" xml:"otickets"`          // Number of assigned override tickets
	FairshareTickets     int     `json:"fairShareTickets" xml:"ftickets"`         // Number of assigned fairshare tickets
	ShareTreeTickets     int     `json:"shareTreeTickets" xml:"stickets"`         // Number of assigned sharetree tickets
	QueueName            string  `json:"queueName" xml:"queue_name"`              // Queue in which the job is executing
	Slots                int     `json:"slots" xml:"slots"`                       // Number of slots
	Tasks                string  `json:"tasks" xml:"tasks"`                       // Task string
}

// NumTasks returns the number of tasks in a QueueJob
func (j QueueJob) NumTasks() int {
	IDRanges, err := ParseTaskIDRanges(j.Tasks)
	if err != nil {
		// Assume any unparseable output is a job with just 1
		return 1
	}
	n := 0
	for _, r := range IDRanges {
		n += r.NumTasks()
	}
	return n
}

// DeletionState returns true if the job is in the (d)eletion state
func (j QueueJob) DeletionState() bool {
	return strings.Contains(j.State, "d")
}

// ErrorState returns true if the job is in the (E)rror state
func (j QueueJob) ErrorState() bool {
	return strings.Contains(j.State, "E")
}

// HoldState returns true if the job is in the (h)old state
func (j QueueJob) HoldState() bool {
	return strings.Contains(j.State, "h")
}

// RunningState returns true if the job is in the (r)unning state
func (j QueueJob) RunningState() bool {
	return strings.Contains(j.State, "r")
}

// RestartedState returns true if the job is in the (R)estarted state
func (j QueueJob) RestartedState() bool {
	return strings.Contains(j.State, "R")
}

// SuspendedState returns true if the job is in the (s)uspended state
func (j QueueJob) SuspendedState() bool {
	return strings.Contains(j.State, "s")
}

// QueueSuspendedState returns true if the job is in the queue (S)uspended state
func (j QueueJob) QueueSuspendedState() bool {
	return strings.Contains(j.State, "S")
}

// TransferringState returns true if the job is in the (t)ransferring state
func (j QueueJob) TransferringState() bool {
	return strings.Contains(j.State, "t")
}

// ThresholdState returns true if the job is in the (T)hreshold state
func (j QueueJob) ThresholdState() bool {
	return strings.Contains(j.State, "T")
}

// WaitingState returns true if the job is in the (w)aiting state
func (j QueueJob) WaitingState() bool {
	return strings.Contains(j.State, "w")
}

// QueuedState returns true if the job is in the (q)ueued state
func (j QueueJob) QueuedState() bool {
	return strings.Contains(j.State, "q")
}

type Queue struct {
	Name          string     `json:"name" xml:"name"`
	QType         string     `json:"qType" xml:"qtype"`
	SlotsUsed     int        `json:"slotsUsed" xml:"slots_used"`
	SlotsReserved int        `json:"slotsReserved" xml:"slots_resv"`
	SlotsTotal    int        `json:"slotsTotal" xml:"slots_total"`
	Arch          string     `json:"arch" xml:"arch"`
	Joblist       []QueueJob `json:"jobList" xml:"job_list"`
}

type QueueInfo struct {
	QueuedJobs  []QueueJob `json:"queuedJobs" xml:"queue_info>job_list"` // A list of jobs currently assigned to queues, eg: executing
	PendingJobs []QueueJob `json:"pendingJobs" xml:"job_info>job_list"`  // A list of jobs that are not yet executing in any queue
}

// absPaths converts the paths of a list of PathList structs in to absolute paths of root if they are not already absolute.
func absPaths(root string, ps []PathList) []PathList {
	var paths []PathList
	for _, p := range ps {
		if !path.IsAbs(p.Path) {
			p.Path = path.Join(root, p.Path)
		}
		paths = append(paths, p)
	}
	return paths
}

func (i *JobInfo) StdoutPaths() []PathList {
	var paths []PathList
	paths = append(paths, absPaths(i.Cwd, i.StdoutPathList)...)
	paths = append(paths, absPaths(i.Cwd, i.AltStdoutPathList)...)
	return paths
}

func (i *JobInfo) StderrPaths() []PathList {
	var paths []PathList
	if !i.MergeStdErr {
		paths = append(paths, absPaths(i.Cwd, i.StderrPathList)...)
		paths = append(paths, absPaths(i.Cwd, i.AltStderrPathList)...)
	}
	return paths
}

func (i *JobInfo) Command() string {
	return i.ScriptFile + " " + strings.Join(i.JobArgs, " ")
}

// Qstat runs qstat -xml with the given arguments and decodes the xml in to result
func Qstat(result interface{}, args ...string) error {
	args = append([]string{"-xml"}, args...)
	cmd := exec.Command("qstat", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("qstat: could not get stdout: %s", err)
	}
	if err = cmd.Start(); err != nil {
		return fmt.Errorf("qstat: could not start qstat: %s", err)
	}
	defer cmd.Wait()
	dec := xml.NewDecoder(util.NewValidUTF8Reader(stdout))
	dec.Strict = false
	if err = dec.Decode(result); err != nil {
		return fmt.Errorf("qstat: could not decode output: %s", err)
	}
	return nil
}

// GetDetailedJobInfo returns a DetailedJobInfo structure contianing all jobs matching the provided pattern.
// The pattern should match the type wc_job_list as defined in man 1 sge_types
func GetDetailedJobInfo(pattern string) (*DetailedJobInfo, error) {
	q := new(DetailedJobInfo)
	err := Qstat(q, "-j", pattern)
	if err != nil {
		// Qstat just produces unparseable XML instead of doing real error reporting. Hurrah.
		if strings.Contains(err.Error(), "XML syntax error on line 3: expected element name after <") {
			return nil, fmt.Errorf("qstat: unknown job: %s", pattern)
		}
		return nil, err
	}
	return q, nil
}

// GetQueueInfo returns a QueueInfo reflecting the current state of the GridEngine queue.
// The argument u can be used to limit the results to a particular user.
// If u is the string "*" then results are returned for all users.
// If u is the empty string then results are returned for the current user.
func GetQueueInfo(u string) (*QueueInfo, error) {
	if u == "" {
		u = "*"
	}
	q := new(QueueInfo)
	err := Qstat(q, "-pri", "-ext", "-urg", "-u", u)
	if err != nil {
		return nil, err
	}
	return q, nil
}
