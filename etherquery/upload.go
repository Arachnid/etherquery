package etherquery

import (
    "bytes"
    "log"
    "math/rand"
    "time"

    "google.golang.org/api/bigquery/v2"
    "google.golang.org/api/googleapi"
)

func backoff(delay time.Duration) func(bool) time.Duration {
    return func(grow bool) (d time.Duration) {
        d = delay + time.Millisecond * time.Duration(rand.Intn(1000))
        if grow {
            delay *= time.Duration(2)
        }
        return
    }
}

func retryRequest(initialDelay, retries int, request func() (interface{}, error)) (interface{}, error) {
    delayer := backoff(time.Second * time.Duration(initialDelay))

    trying: for {
        result, err := request()
        if err == nil {
            return result, err
        }

        switch err := err.(type) {
        case *googleapi.Error:
            if err.Code / 500 == 5 {
                delay := delayer(true)
                log.Printf("Got status %v, backing off for %v before retrying.", err.Code, delay)
                time.Sleep(delay)
                continue trying
            }
        }

        if retries > 0 {
            delay := delayer(false)
            log.Printf("Got error %v, retrying after %v seconds.", err, delay)
            time.Sleep(delay)
            retries -= 1
        } else {
            return result, err
        }
    }

}

func uploadData(service *bigquery.Service, project string, dataset string, table string, recordCount int, data []byte) {
    start := time.Now()

    job := &bigquery.Job{
        Configuration: &bigquery.JobConfiguration{
            Load: &bigquery.JobConfigurationLoad{
                DestinationTable: &bigquery.TableReference{
                    ProjectId: project,
                    DatasetId: dataset,
                    TableId:   table,
                },
                SourceFormat: "NEWLINE_DELIMITED_JSON",
                WriteDisposition: "WRITE_APPEND",
            },
        },
    }

    // Try starting the job until it succeeds or we give up
    j, err := retryRequest(1, 3, func() (interface{}, error) {
        call := service.Jobs.Insert(project, job).
            Media(bytes.NewReader(data), googleapi.ContentType("text/json"))
        return call.Do()
    })
    if err != nil {
        log.Printf("Got error %v while submitting job; giving up.", err)
        return
    }
    job = j.(*bigquery.Job)

    // Poll the job until it's done
    for {
        j, err := retryRequest(1, 3, func() (interface{}, error) {
            return service.Jobs.Get(project, job.JobReference.JobId).Do()
        })
        if err != nil {
            log.Printf("Got error %v while polling for job status; giving up.", err)
            return
        }
        job = j.(*bigquery.Job)
        if job.Status.State == "DONE" {
            break
        }
        time.Sleep(time.Second * 5)
    }

    if res := job.Status.ErrorResult; res != nil {
        log.Printf("Job failed with error: %v", res)
        for i := 0; i < len(job.Status.Errors); i++ {
            log.Printf("  %v", job.Status.Errors[i])
        }
        return
    }
    log.Printf("Successfully inserted %v records after %v", recordCount, time.Since(start))
}
