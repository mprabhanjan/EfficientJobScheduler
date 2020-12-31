package main

import (
    "container/heap"
    "errors"
    "fmt"
    "log"
    "sync"
    "time"
)

type TaskCall func(params interface{})

type task struct {
    call_fn TaskCall
    exec_at time.Time
    reg_tstamp time.Time
    index int
    params interface{}
    job_id uint64
}

type PrioQ []*task

func (prq PrioQ) Len() int {
    return len(prq)
}

func (prq PrioQ) Less(i, j int) bool {
    if prq[i].exec_at.Before(prq[j].exec_at) {
        return true
    }
    if prq[i].exec_at.After(prq[j].exec_at) {
        return false
    }
    return prq[i].reg_tstamp.Before(prq[j].reg_tstamp)
}

func (prq PrioQ) Swap(i, j int) {
    prq[i], prq[j] = prq[j], prq[i]
    prq[i].index = i
    prq[j].index = j
}

func (prq *PrioQ) Push(x interface{}) {
    n := len(*prq)
    item := x.(*task)
    item.index = n
    *prq = append(*prq, item)
}

func (prq *PrioQ) Pop() interface{} {
    old := *prq
    n := len(old)
    item := old[n-1]
    old[n-1] = nil  // avoid memory leak
    item.index = -1 // for safety
    *prq = old[0 : n-1]
    return item
}

type Scheduler struct {
    cond_var sync.Cond
    mutex sync.Mutex
    prioq PrioQ
    timer *time.Timer
    timerStarted bool
    next_firingTime time.Time
    timer_schedule_signal chan time.Time
    job_id uint64
}

func NewTimer() *time.Timer {
    t := time.NewTimer(time.Second)
    StopAndDrainTimer(t)
    return t
}

func StopAndDrainTimer(t *time.Timer) {
    t.Stop()
    for len(t.C) > 0 {
        <-t.C
    }
}

func StopAndResetTimer(t *time.Timer, d time.Duration) {
    StopAndDrainTimer(t)
    t.Reset(d)
}

func NewScheduler(wg *sync.WaitGroup) *Scheduler  {

    scheduler := &Scheduler{
        mutex: sync.Mutex{},
        cond_var: sync.Cond{
        },
        prioq: make([]*task, 0),
        timer: NewTimer(),
        timer_schedule_signal: make(chan time.Time),
    }
    scheduler.cond_var.L = &scheduler.mutex

    heap.Init(&scheduler.prioq)
    go scheduler.cv_signaler()
    go scheduler.execute_tasks()
    wg.Add(2)
    return scheduler
}

func (sched *Scheduler) nextJobId() uint64 {
    sched.job_id += 1
    return sched.job_id
}

func (sched *Scheduler) cv_signaler() {
    for {
        select {
        case <- sched.timer.C:
            sched.timerStarted = false
            //log.Printf("cv_signaler: Rx Timer-Expiry!. Giving signal to CV....")
            sched.cond_var.Signal()

        case new_firing_time := <- sched.timer_schedule_signal:
            //log.Printf("cv_signaler: Rx new_firing_time=%s. timerStarted=%t, next_firing_time=%s",
            //    new_firing_time, sched.timerStarted, sched.next_firingTime)
            if !time.Now().Before(new_firing_time) {
                log.Printf("cv_signalar:: Unpected! Giving signal right away!")
                sched.cond_var.Signal()
            } else if !sched.timerStarted || sched.next_firingTime.After(new_firing_time) {
                StopAndResetTimer(sched.timer, new_firing_time.Sub(time.Now()))
                sched.next_firingTime = new_firing_time
                sched.timerStarted = true
                //log.Printf("cv_signaler:: set next_firing_time to %s\n", sched.next_firingTime)
            }
        }
    }
}

func (sched *Scheduler) RegisterTaskCall(taskcall TaskCall, params interface{}, at *time.Time) error {

    t_now := time.Now()
    if at.Before(t_now) {
        msg := fmt.Sprintf("Execution time %s has elapsed before registration!", at)
        return errors.New(msg)
    }

    new_task := &task {
        call_fn: taskcall,
        exec_at: *at,
        reg_tstamp: t_now,
        params: params,
        job_id: sched.nextJobId(),
    }

    sched.mutex.Lock()
    heap.Push(&sched.prioq, new_task)
    sched.mutex.Unlock()
    //log.Printf("RegisterTaskCall: New task scheduled at %s", *at)

    sched.timer_schedule_signal <- *at
    return nil
}

func (sched *Scheduler) execute_tasks() {
    for {
        sched.mutex.Lock()
        t := time.Now()
        for len(sched.prioq) == 0 || (len(sched.prioq) > 0 && sched.prioq[0].exec_at.After(t)) {
            sched.cond_var.Wait()
            t = time.Now()
        }

        //log.Printf("execute_tasks: Executing Tasks....")
        for len(sched.prioq) > 0 && !sched.prioq[0].exec_at.After(t) {
            t := heap.Pop(&sched.prioq)
            task, ok := t.(*task)
            if ok {
                log.Printf("Executing task with task_id=%d\n", task.job_id)
                go task.call_fn(task.params)
            }
        }
        sched.mutex.Unlock()
        if len(sched.prioq) > 0 {
            //log.Printf("execute_tasks: Sending top of prioq time %s\n", sched.prioq[0].exec_at)
            sched.timer_schedule_signal <- sched.prioq[0].exec_at
        }
    }
}