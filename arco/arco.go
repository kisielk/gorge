package arco

import (
	"database/sql"
	pq "github.com/bmizerany/pq"
	"time"
)

type DB struct {
	db *sql.DB
}

type Job struct {
	JobNumber      int
	TaskNumber     int
	PETaskId       string
	JobName        string
	Group          string
	Owner          string
	Account        string
	Priority       string
	SubmissionTime time.Time
	Project        string
	Department     string
}

type Accounting struct {
	JobNumber      int
	TaskNumber     int
	PETaskId       string
	Name           string
	Group          string
	Username       string
	Account        string
	Project        string
	Department     string
	SubmissionTime time.Time
	ARParent       int
	StartTime      time.Time
	EndTime        time.Time
	WallClockTime  int
	CPU            float64
	Memory         float64
	IO             float64
	IOWait         float64
	MaxVMem        float64
	ExitStatus     int
	MaxRSS         int
}

const jobQuery = `SELECT j_job_number, j_task_number, j_pe_taskid, j_job_name, j_group, j_owner, 
j_account, j_priority, j_submission_time, j_project, j_department 
FROM sge_job 
WHERE j_job_number = $1 AND j_task_number = -1 
ORDER BY j_job_number DESC`

const accountingQuery = `SELECT job_number, task_number, pe_taskid, name, \"group\",
username, account, project, department, submission_time, ar_parent, start_time, end_time,
wallclock_time, cpu, mem, io, iow, maxvmem, exit_status, maxrss,
FROM view_accounting 
WHERE job_number = $1
ORDER BY task_number`

// Open creates a new connection to the Arco database.
// URL is a Postgres connection string in the form:
//    "postgres://bob:secret@1.2.3.4:5432/mydb?option=value"
func Open(url string) (*DB, error) {
	dsn, err := pq.ParseURL(url)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("postgres", dsn)
	return &DB{db}, err
}

// QueryJob queries the job table for information about a job number
func (d DB) QueryJob(n int) (*Job, error) {
	s, err := d.db.Prepare(jobQuery)
	if err != nil {
		return nil, err
	}

	var j Job
	r, err := s.Query(n)
	if err != nil {
		return nil, err
	}

	for r.Next() {
		err = r.Scan(&j.JobNumber, &j.TaskNumber, &j.PETaskId, &j.JobName, &j.Group, &j.Owner,
			&j.Account, &j.Priority, &j.SubmissionTime, &j.Project, &j.Department)
		if err != nil {
			return nil, err
		}
	}

	return &j, nil
}

// QueryAccounting queries the view_accounting view for accounting information for a job number j. 
func (d DB) QueryAccounting(j int) ([]Accounting, error) {
	s, err := d.db.Prepare(accountingQuery)
	if err != nil {
		return nil, err
	}

	r, err := s.Query(j)

	var as []Accounting

	for r.Next() {
		var a Accounting
		err = r.Scan(&a.JobNumber, &a.TaskNumber, &a.PETaskId, &a.Name, &a.Group, &a.Username,
			&a.Account, &a.Project, &a.Department, &a.SubmissionTime, &a.ARParent, &a.StartTime,
			&a.EndTime, &a.WallClockTime, &a.CPU, &a.Memory, &a.IO, &a.IOWait, &a.MaxVMem, &a.ExitStatus, &a.MaxRSS)
		if err != nil {
			return nil, err
		}
		as = append(as, a)
	}

	return as, nil
}
