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

const defaultReplicas = 50
const defaultDuration = time.Second

//Dcron is main struct
type Dcron struct {
	jobs       map[string]*JobWarpper
	mu         sync.RWMutex
	ServerName string
	nodePool   *NodePool
	isRun      bool

	logger *logrus.Logger

	nodeUpdateDuration time.Duration
	hashReplicas       int

	cr        *cron.Cron
	crOptions []cron.Option
}

//NewDcron create a Dcron
func NewDcron(serverName string, driver driver.Driver, cronOpts ...cron.Option) *Dcron {
	dcron := newDcron(serverName)
	dcron.crOptions = cronOpts
	dcron.cr = cron.New(dcron.crOptions...)
	dcron.nodePool = newNodePool(serverName, driver, dcron, dcron.nodeUpdateDuration, dcron.hashReplicas)
	return dcron
}

//NewDcronWithOption create a Dcron with Dcron Option
func NewDcronWithOption(serverName string, driver driver.Driver, dcronOpts ...Option) *Dcron {
	dcron := newDcron(serverName)
	for _, opt := range dcronOpts {
		opt(dcron)
	}

	dcron.cr = cron.New(dcron.crOptions...)
	dcron.nodePool = newNodePool(serverName, driver, dcron, dcron.nodeUpdateDuration, dcron.hashReplicas)
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
		ServerName:         serverName,
		logger:             log,
		jobs:               make(map[string]*JobWarpper),
		crOptions:          make([]cron.Option, 0),
		nodeUpdateDuration: defaultDuration,
		hashReplicas:       defaultReplicas,
	}
}

//SetLogger set dcron logger
func (d *Dcron) SetLogger(logger *logrus.Logger) {
	d.logger = logger
}

//GetLogger get dcron logger
func (d *Dcron) GetLogger() *logrus.Logger {
	return d.logger
}

func (d *Dcron) info(format string, v ...interface{}) {
	d.logger.Infof(format, v...)
}
func (d *Dcron) err(format string, v ...interface{}) {
	d.logger.Errorf(format, v...)
}

//AddJob  add a job
func (d *Dcron) AddJob(jobName, cronStr string, job Job) (err error) {
	return d.addJob(jobName, cronStr, nil, job)
}

//AddFunc add a cron func
func (d *Dcron) AddFunc(jobName, cronStr string, cmd func()) (err error) {
	return d.addJob(jobName, cronStr, cmd, nil)
}
func (d *Dcron) addJob(jobName, cronStr string, cmd func(), job Job) (err error) {
	d.info("addJob '%s' :  %s", jobName, cronStr)
	if _, ok := d.jobs[jobName]; ok {
		return errors.New("jobName already exist")
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
		return err
	}
	innerJob.ID = entryID
	d.jobs[jobName] = &innerJob

	return nil
}

// Remove Job
func (d *Dcron) Remove(jobName string) {
	if job, ok := d.jobs[jobName]; ok {
		delete(d.jobs, jobName)
		d.cr.Remove(job.ID)
	}
}

// GetJobEntryID 获取job执行的ID
func (d *Dcron) GetJobEntryID(jobName string) cron.EntryID {
	if job, ok := d.jobs[jobName]; ok {
		return job.ID
	}

	return 0
}

// Next 获取job下次执行时间
func (d *Dcron) Next(jobName string) time.Time {
	if job, ok := d.jobs[jobName]; ok {
		return d.cr.Entry(job.ID).Next
	}

	return time.Time{}
}

// HealthCheck 检查当前任务是否在运行中
func (d *Dcron) HealthCheck(jobName string) cron.Entry {
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
	err := d.nodePool.StartPool()
	if err != nil {
		d.isRun = false
		d.err("dcron start node pool error %+v", err)
		return
	}
	d.cr.Start()
	d.info("dcron started , nodeID is %s", d.nodePool.NodeID)
}

// Run Job
func (d *Dcron) Run() {
	d.isRun = true
	err := d.nodePool.StartPool()
	if err != nil {
		d.isRun = false
		d.err("dcron start node pool error %+v", err)
		return
	}
	d.cr.Run()
	d.info("dcron running nodeID is %s", d.nodePool.NodeID)
}

//Stop stop job
func (d *Dcron) Stop() {
	d.isRun = false
	d.cr.Stop()
	d.info("dcron stopped")
}
