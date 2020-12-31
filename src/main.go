package main

import (
    "fmt"
    "sync"
    "time"
)

func main() {

    wg := sync.WaitGroup{}
    job_scheduler := NewScheduler(&wg)

    for {
        t1 := time.Now().Add(time.Second * 10)
        job_scheduler.RegisterTaskCall(JobTypeOne, &t1, &t1)

        t2 := time.Now().Add(time.Second * 30)
        job_scheduler.RegisterTaskCall(JobTypeOne, &t2, &t2)

        t3 := time.Now().Add(time.Second * 60)
        job_scheduler.RegisterTaskCall(JobTypeOne, &t3, &t3)

        t4 := time.Now().Add(time.Microsecond * 800)
        job_scheduler.RegisterTaskCall(JobTypeOne, &t4, &t4)

        //Again t1
        job_scheduler.RegisterTaskCall(JobTypeOne, &t1, &t1)

        time.Sleep(time.Second * 30)
    }

    wg.Wait()
}


func JobTypeOne(param interface{}) {

    t_now := time.Now()
    expected_time, ok := param.(*time.Time)
    if ok {
        fmt.Printf("JobTypeOne: Expected execution time %s. Actual executed time %s\n",
            expected_time, t_now)
    } else {
        fmt.Printf("JobTypeOne: Unable to gather the param passed!. Actual executed time %s\n",
            expected_time, t_now)
    }
}