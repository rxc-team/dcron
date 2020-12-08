package dcron

import (
	"errors"
	"os"
	"sync"
	"time"

	nested "github.com/antonfisher/nested-logrus-formatter"
	"github.com/robfig/cron/v3"
	"github.com/rxc-team/dcron/driver"
	"github.com/sirupsen/logrus"
)

//Dcron is main struct
type Dcron struct {
	jobs       map[string]*JobWarpper
	mu         sync.RWMutex
	cr         *cron.Cron
	ServerName string
	nodePool   *NodePool
	logger     *logrus.Logger
	isRun      bool
}

//NewDcron create a Dcron
func NewDcron(serverName string, driver driver.Driver, opts ...cron.Option) *Dcron {

	dcron := newDcron(serverName)
	dcron.cr = cron.New(opts...)
	dcron.nodePool = newNodePool(serverName, driver, dcron)
	return dcron
}

func newDcron(serverName string) *Dcron {
	log := logrus.New()

	log.Out = os.Stdout
	log.Level = logrus.InfoLevel
	formatter := &nested.Formatter{
		HideKeys:        true,
		NoFieldsColors:  false,
		NoColors:        false,
		TimestampFormat: "2006-01-02 15:04:05",
	}

	log.SetFormatter(formatter)

	return &Dcron{
		ServerName: serverName,
		logger:     log,
		jobs:       make(map[string]*JobWarpper),
	}
}

func (d *Dcron) info(format string, v ...interface{}) {
	d.logger.WithFields(logrus.Fields{
		"log_type": "JOB",
	}).Infof(format, v...)
}
func (d *Dcron) err(format string, v ...interface{}) {
	d.logger.WithFields(logrus.Fields{
		"log_type": "JOB",
	}).Errorf(format, v...)
}

//AddJob  add a job
func (d *Dcron) AddJob(jobName, cronStr string, job Job) (id cron.EntryID, err error) {
	return d.addJob(jobName, cronStr, nil, job)
}

//AddFunc add a cron func
func (d *Dcron) AddFunc(jobName, cronStr string, cmd func()) (id cron.EntryID, err error) {
	return d.addJob(jobName, cronStr, cmd, nil)
}
func (d *Dcron) addJob(jobName, cronStr string, cmd func(), job Job) (id cron.EntryID, err error) {
	d.info("addJob '%s' :  %s", jobName, cronStr)
	if _, ok := d.jobs[jobName]; ok {
		return 0, errors.New("jobName already exist")
	}
	innerJob := JobWarpper{
		Name:    jobName,
		CronStr: cronStr,
		Func:    cmd,
		Job:     job,
		Dcron:   d,
	}
	entryID, err := d.cr.AddJob(cronStr, innerJob)
	if err != nil {
		return 0, err
	}
	innerJob.ID = entryID
	d.jobs[jobName] = &innerJob
	return entryID, nil

}

// Remove Job
func (d *Dcron) Remove(jobName string) {
	if job, ok := d.jobs[jobName]; ok {
		delete(d.jobs, jobName)
		d.cr.Remove(job.ID)
	}
}

// Next 获取job下次执行时间
func (d *Dcron) Next(jobName string) time.Time {
	if job, ok := d.jobs[jobName]; ok {
		return d.cr.Entry(job.ID).Next
	}

	return time.Time{}
}

// GetJobInfo 获取当前任务详情
func (d *Dcron) GetJobInfo(jobName string) cron.Entry {
	if job, ok := d.jobs[jobName]; ok {
		return d.cr.Entry(job.ID)
	}

	return cron.Entry{}
}

func (d *Dcron) allowThisNodeRun(jobName string) bool {
	allowRunNode := d.nodePool.PickNodeByJobName(jobName)
	d.info("job '%s' running in node %s", jobName, allowRunNode)
	if allowRunNode == "" {
		d.err("node pool is empty")
		return false
	}
	return d.nodePool.NodeID == allowRunNode
}

//Start start job
func (d *Dcron) Start() {
	d.isRun = true
	d.cr.Start()
	d.info("dcron started , nodeID is %s", d.nodePool.NodeID)
}

// Run Job
func (d *Dcron) Run() {
	d.isRun = true
	d.cr.Run()
	d.info("dcron running nodeID is %s", d.nodePool.NodeID)
}

//Stop stop job
func (d *Dcron) Stop() {
	d.isRun = false
	d.cr.Stop()
	d.info("dcron stopped")
}
