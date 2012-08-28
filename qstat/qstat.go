// Package qstat provides an interface to GridEngine's job and queue status facilities
package qstat

import (
	"encoding/xml"
	"errors"
	"github.com/kisielk/gorge/util"
	"os/exec"
	"path"
	"strings"
)

// Resource represents a GridEngine resource request
// See man 5 sge_complex for a more detailed description of the fields
type Resource struct {
	Name        string  `xml:"CE_name"`         // The name of the complex resource
	ValType     int     `xml:"CE_valtype"`      // The type of value
	StringVal   string  `xml:"CE_stringval"`    // The value as a string
	DoubleVal   float64 `xml:"CE_doubleval"`    // The value as a double
	RelOp       int     `xml:"CE_relop"`        // The relation operator used to compare the value
	Consumable  bool    `xml:"CE_consumable"`   // True if the resource is a consumable resourece
	Dominant    bool    `xml:"CE_dominant"`     // ?
	PJDoubleVal float64 `xml:"CE_pj_doubleval"` // ?
	PJDominant  bool    `xml:"CE_pj_dominant"`  // ?
	Requestable bool    `xml:"CE_requestable"`  // True if the resource is a requestable resource
	Tagged      bool    `xml:"CE_tagged"`       // ?
}

// MailAddress represents an email address
type MailAddress struct {
	User string `xml:"MR_user"`
	Host string `xml:"MR_host"`
}

// String implements the Stringer interface
func (a MailAddress) String() string {
	return a.User + "@" + a.Host
}

// EnvVar represents a job environment variable
type EnvVar struct {
	Variable string `xml:"VA_variable"` // The name of the variable
	Value    string `xml:"VA_value"`    // The value of the variable
}

type PathList struct {
	Path        string `xml:"PN_path"`
	Host        string `xml:"PN_host"`
	FileHost    string `xml:"PN_file_host"`
	FileStaging bool   `xml:"PN_file_staging"`
}

// TaskIDRange represents a range of job array task identifiers
type TaskIDRange struct {
	Min  int `xml:"RN_min"`  // The minimum task ID
	Max  int `xml:"RN_max"`  // The maximum task ID
	Step int `xml:"RN_step"` // The ID step size between tasks
}

type JATMessage struct {
	Type    int    `xml:"QIM_type"`
	Message string `xml:"QIM_message"`
}

type Messages struct {
	Messages       []SMEMessage `xml:"SME_message_list>element"`
	GlobalMessages []SMEMessage `xml:"SME_global_message_list>element"`
}

type SMEMessage struct {
	JobNumbers []int  `xml:"MES_job_number_list>ulong_sublist>ULNG_value"`
	Number     int    `xml:"MES_message_number"`
	Message    string `xml:"MES_message"`
}

type Task struct {
	Status      int        `xml:"JAT_status"`
	TaskNumber  int        `xml:"JAT_task_number"`
	MessageList JATMessage `xml:"JAT_message_list>ulong_sublist"`
}

type JobInfo struct {
	JobNumber            int           `xml:"JB_job_number"`
	AdvanceReservation   int           `xml:"JB_ar"`
	ExecFile             string        `xml:"JB_exec_file"`
	SubmissionTime       int           `xml:"JB_submission_time"`
	Owner                string        `xml:"JB_owner"`
	Uid                  int           `xml:"JB_uid"`
	Group                string        `xml:"JB_group"`
	Gid                  int           `xml:"JB_gid"`
	Account              string        `xml:"JB_account"`
	MergeStdErr          bool          `xml:"JB_merge_stderr"`
	MailList             []MailAddress `xml:"JB_mail_list>element"`
	Project              string        `xml:"JB_project"`
	Notify               bool          `xml:"JB_notify"`
	JobName              string        `xml:"JB_job_name"`
	StdoutPathList       []PathList    `xml:"JB_stdout_path_list>path_list"`
	AltStdoutPathList    []PathList    `xml:"JB_stdout_path_list>stdout_path_list"` // Alternate stdout path list
	JobShare             int           `xml:"JB_jobshare"`
	HardResourceList     []Resource    `xml:"JB_hard_resource_list>qstat_l_requests"`
	EnvList              []EnvVar      `xml:"JB_env_list>job_sublist"`
	JobArgs              []string      `xml:"JB_job_args>element>ST_name"`
	ScriptFile           string        `xml:"JB_script_file"`
	JobArrayTasks        []Task        `xml:"JB_ja_tasks>ulong_sublist"`
	Cwd                  string        `xml:"JB_cwd"`
	StderrPathList       []PathList    `xml:"JB_stderr_path_list>path_list"`
	AltStderrPathList    []PathList    `xml:"JB_stderr_path_list>stderr_path_list"` // Alternate stderr path list
	JIDRequestList       []int         `xml:"JB_jid_request_list>element>JRE_job_name"`
	JIDSuccessorList     []int         `xml:"JB_jid_successor_list>ulong_sublist>JRE_job_number"`
	Deadline             bool          `xml:"JB_deadline"`
	ExecutionTime        int           `xml:"JB_execution_time"`
	CheckpointAttr       int           `xml:"JB_checkpoint_attr"`
	CheckpointInterval   int           `xml:"JB_checkpoint_interval"`
	Reserve              bool          `xml:"JB_reserve"`
	MailOptions          int           `xml:"JB_mail_options"`
	Priority             int           `xml:"JB_priority"`
	Restart              int           `xml:"JB_restart"`
	Verify               bool          `xml:"JB_verify"`
	ScriptSize           int           `xml:"JB_script_size"`
	VerifySuitableQueues bool          `xml:"JB_verify_suitable_queues"`
	SoftWallClockGMT     int           `xml:"JB_soft_wallclock_gmt"`
	HardWallClockGMT     int           `xml:"JB_hard_wallclock_gmt"`
	OverrideTickets      int           `xml:"JB_override_tickets"`
	Version              int           `xml:"JB_version"`
	JobArray             TaskIDRange   `xml:"JB_ja_structure>task_id_range"`
	Type                 int           `xml:"JB_type"`
}

// Type DetailedJobInfo represents the job information returned by qstat -j
type DetailedJobInfo struct {
	Jobs     []JobInfo `xml:"djob_info>element"`
	Messages Messages  `xml:"messages>element"`
}

type QueueJob struct {
	JobNumber          int     `xml:"JB_job_number"`
	NormalizedPriority float32 `xml:"JAT_prio"`
	POSIXPriority      int     `xml:"JB_priority"`
	Name               string  `xml:"JB_name"`
	Owner              string  `xml:"JB_owner"`
	Project            string  `xml:"JB_project"`
	Department         string  `xml:"JB_department"`
	State              string  `xml:"state"`
	StartTime          string  `xml:"JAT_start_time"`
	CpuUsage           float64 `xml:"cpu_usage"`
	MemUsage           float64 `xml:"mem_usage"`
	IOUsage            float64 `xml:"io_usage"`
	Tickets            int     `xml:"tickets"`
	OverrideTickets    int     `xml:"otickets"`
	FairshareTickets   int     `xml:"ftickets"`
	ShareTreeTickets   int     `xml:"stickets"`
	QueueName          string  `xml:"queue_name"`
	Slots              int     `xml:"slots"`
}

func (j QueueJob) Error() bool {
	return strings.Contains(j.State, "E")
}

type Queue struct {
	Name          string     `xml:"name"`
	QType         string     `xml:"qtype"`
	SlotsUsed     int        `xml:"slots_used"`
	SlotsReserved int        `xml:"slots_resv"`
	SlotsTotal    int        `xml:"slots_total"`
	Arch          string     `xml:"arch"`
	Joblist       []QueueJob `xml:"job_list"`
}

type QueueInfo struct {
	QueuedJobs  []QueueJob `xml:"queue_info>job_list"` // A list of jobs currently assigned to queues, eg: executing
	PendingJobs []QueueJob `xml:"job_info>job_list"`   // A list of jobs that are not yet executing in any queue
}

// Function absPaths converts the paths of a list of PathList structs in to absolute paths of root if they are not already absolute.
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

// GetDetailedJobInfo returns a DetailedJobInfo structure contianing all jobs matching the provided pattern.
// The pattern should match the type wc_job_list as defined in man 1 sge_types
func GetDetailedJobInfo(pattern string) (q *DetailedJobInfo, err error) {
	cmd := exec.Command("qstat", "-j", pattern, "-xml")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	if err = cmd.Start(); err != nil {
		return nil, err
	}

	d := xml.NewDecoder(util.NewValidUTF8Reader(stdout))
	d.Strict = false
	if err = d.Decode(&q); err != nil {
		// Qstat just produces unparseable XML instead of doing real error reporting. Hurrah.
		if err.Error() == "XML syntax error on line 3: expected element name after <" {
			return nil, errors.New("Unknown job: " + pattern)
		}
	}

	if err = cmd.Wait(); err != nil {
		return nil, err
	}

	return q, nil
}

// GetQueueInfo returns a QueueInfo reflecting the current state of the GridEngine queue.
// The argument u can be used to limit the results to a particular user.
// If u is the string "*" then results are returned for all users.
// If u is the empty string then results are returned for the current user.
func GetQueueInfo(u string) (q *QueueInfo, err error) {
	args := []string{"-xml", "-pri", "-ext"}

	if u != "" {
		args = append(args, "-u", "*")
	}

	cmd := exec.Command("qstat", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	if err = cmd.Start(); err != nil {
		return nil, err
	}
	d := xml.NewDecoder(util.NewValidUTF8Reader(stdout))
	d.Strict = false

	if err = d.Decode(&q); err != nil {
		return nil, err
	}

	if err = cmd.Wait(); err != nil {
		return nil, err
	}

	return q, nil
}
