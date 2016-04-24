package etherquery

import (
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
            if err.Code / 500 == 5 || err.Code == 403 {
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

func uploadData(service *bigquery.Service, project string, dataset string, table string, rows []*bigquery.TableDataInsertAllRequestRows) {
    start := time.Now()

    request := &bigquery.TableDataInsertAllRequest{
        Rows: rows,
    }

    // Try inserting the data until it succeeds or we give up
    r, err := retryRequest(1, 3, func() (interface{}, error) {
        call := service.Tabledata.InsertAll(project, dataset, table, request)
        return call.Do()
    })
    if err != nil {
        log.Printf("Got error %v while submitting job; giving up.", err)
        return
    }
    response := r.(*bigquery.TableDataInsertAllResponse)

    if res := response.InsertErrors; len(res) > 0 {
        log.Printf("Got %v insert errors:", len(res))
        for i := 0; i < len(res); i++ {
            log.Printf("  %v", res[i])
        }
        return
    }
    log.Printf("Successfully inserted %v records after %v", len(rows), time.Since(start))
}
