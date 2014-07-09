// Copyright 2012 Kamil Kisiel. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package arco provides functions for accessing a gridengine ARCo database
package arco

import (
	"database/sql"
	pq "github.com/lib/pq"
	"time"
)

type scannable interface {
	Scan(v ...interface{}) error
}

type DB struct {
	db *sql.DB
}

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

const jobQuery = `SELECT j_job_number, j_task_number, j_pe_taskid, j_job_name, j_group, j_owner, 
j_account, j_priority, j_submission_time, j_project, j_department 
FROM sge_job 
WHERE j_job_number = $1 AND j_task_number = -1 
ORDER BY j_job_number DESC`

type Job struct {
	JobNumber      int       `json:"jobNumber"`
	TaskNumber     int       `json:"taskNumber"`
	PETaskId       string    `json:"peTaskId"`
	JobName        string    `json:"jobName"`
	Group          string    `json:"group"`
	Owner          string    `json:"owner"`
	Account        string    `json:"account"`
	Priority       string    `json:"priority"`
	SubmissionTime time.Time `json:"submissionTime"`
	Project        string    `json:"project"`
	Department     string    `json:"department"`
}

// QueryJob queries the job table for information about a job number
func (d DB) QueryJob(n int) (*Job, error) {
	var j Job
	r := d.db.QueryRow(jobQuery, n)
	err := r.Scan(&j.JobNumber, &j.TaskNumber, &j.PETaskId, &j.JobName, &j.Group, &j.Owner,
		&j.Account, &j.Priority, &j.SubmissionTime, &j.Project, &j.Department)
	return &j, err
}

type Accounting struct {
	JobNumber      int       `json:"jobNumber"`
	TaskNumber     int       `json:"taskNumber"`
	PETaskId       string    `json:"peTaskId"`
	Name           string    `json:"jobName"`
	Group          string    `json:"group"`
	Username       string    `json:"userName"`
	Account        string    `json:"account"`
	Project        string    `json:"project"`
	Department     string    `json:"department"`
	SubmissionTime time.Time `json:"submissionTime"`
	ARParent       int       `json:"arParent"`
	StartTime      time.Time `json:"startTime"`
	EndTime        time.Time `json:"endTime"`
	WallClockTime  int       `json:"wallClockTime"`
	CPU            float64   `json:"cpu"`
	Memory         float64   `json:"memory"`
	IO             float64   `json:"io"`
	IOWait         float64   `json:"ioWait"`
	MaxVMem        float64   `json:"maxVmem"`
	ExitStatus     int       `json:"exitStatus"`
	MaxRSS         int       `json:"maxRss"`
}

// scanAccounting scans a scannable in to an Accounting struct
func scanAccounting(r scannable) (*Accounting, error) {
	var a Accounting
	err := r.Scan(&a.JobNumber, &a.TaskNumber, &a.PETaskId, &a.Name, &a.Group, &a.Username,
		&a.Account, &a.Project, &a.Department, &a.SubmissionTime, &a.ARParent, &a.StartTime,
		&a.EndTime, &a.WallClockTime, &a.CPU, &a.Memory, &a.IO, &a.IOWait, &a.MaxVMem, &a.ExitStatus, &a.MaxRSS)
	return &a, err
}

const accountingQuery = `SELECT job_number, task_number, pe_taskid, name, \"group\",
username, account, project, department, submission_time, ar_parent, start_time, end_time,
wallclock_time, cpu, mem, io, iow, maxvmem, exit_status, maxrss,
FROM view_accounting 
WHERE job_number = $1
ORDER BY task_number`

// QueryAccounting queries the view_accounting view for accounting information for a job number j.
// It returns accounting records for all tasks.
func (d DB) QueryAccounting(j int) ([]Accounting, error) {
	rows, err := d.db.Query(accountingQuery, j)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var as []Accounting

	for rows.Next() {
		a, err := scanAccounting(rows)
		if err != nil {
			return nil, err
		}
		as = append(as, *a)
	}

	return as, rows.Err()
}

const accountingTaskQuery = `SELECT job_number, task_number, pe_taskid, name, \"group\",
username, account, project, department, submission_time, ar_parent, start_time, end_time,
wallclock_time, cpu, mem, io, iow, maxvmem, exit_status, maxrss,
FROM view_accounting 
WHERE job_number = $1 AND task_number = $2`

// QueryAccountingTask queries the view_accounting view for accounting information of a task t of a job j.
func (d DB) QueryAccountingTask(j, t int) (*Accounting, error) {
	row := d.db.QueryRow(accountingTaskQuery, j, t)
	return scanAccounting(row)
}

const accountingTimesQuery = `SELECT job_number, task_number, pe_taskid, name, \"group\",
username, account, project, department, submission_time, ar_parent, start_time, end_time,
wallclock_time, cpu, mem, io, iow, maxvmem, exit_status, maxrss,
FROM view_accounting 
WHERE start_time < $1 AND end_time > $2
ORDER BY job_number, task_number, pe_taskid`

// QueryAccountingTimes queries the view_accounting view for all accounting records of jobs that ran in a given
// time period.
func (d DB) QueryAccountingTimes(start, end time.Time) ([]Accounting, error) {
	rows, err := d.db.Query(accountingTimesQuery, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var as []Accounting

	for rows.Next() {
		a, err := scanAccounting(rows)
		if err != nil {
			return nil, err
		}
		as = append(as, *a)
	}

	return as, rows.Err()
}

type Log struct {
	JobNumber  int       `json:"jobNumber"`
	TaskNumber int       `json:"taskNumber"`
	PETaskId   int       `json:"peTaskId"`
	JobName    string    `json:"jobName"`
	User       string    `json:"user"`
	Account    string    `json:"account"`
	Project    string    `json:"project"`
	Department string    `json:"department"`
	Time       time.Time `json:"time"`
	Event      string    `json:"event"`
	State      string    `json:"state"`
	Initiator  string    `json:"initiator"`
	Host       string    `json:"host"`
	Message    string    `json:"message"`
}

const logQuery = `SELECT job_number, task_number, pe_taskid, name, user, account, project, department,
time, event, state, initiator, host, message
FROM view_job_log_ordered
WHERE job_number = $1 AND task_number = $2`

// QueryLogs returns a list of all log entries for a job and task number. A task number of -1 returns a log summary for an
// array job.
func (d DB) QueryLogs(j, t int) ([]Log, error) {
	rows, err := d.db.Query(logQuery, j, t)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []Log

	for rows.Next() {
		var l Log
		err := rows.Scan(&l.JobNumber, &l.TaskNumber, &l.PETaskId, &l.JobName, &l.User, &l.Account, &l.Project, &l.Department,
			&l.Time, &l.Event, &l.State, &l.Initiator, &l.Host, &l.Message)
		if err != nil {
			return nil, err
		}
		logs = append(logs, l)
	}

	return logs, err
}
