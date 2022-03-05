package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	_ "net/http/pprof"

	ws "github.com/gorilla/websocket"
)

type HubMessage struct {
	Source          string `json:"source"`
	Name            string `json:"name"`
	DisplayName     string `json:"displayName"`
	Value           string `json:"value"`
	Type            string `json:"type"`
	Unit            string `json:"unit"`
	DeviceID        int    `json:"deviceId"`
	HubID           int    `json:"hubId"`
	InstalledAppID  int    `json:"installedAppId"`
	DescriptionText string `json:"descriptionText"`
	Msg             string `json:"msg"`
	ID              int    `json:"id"`
	Time            string `json:"time"`
	Level           string `json:"level"`
}

type HubEvent struct {
	Source          string `json:"source"`
	Name            string `json:"name"`
	DisplayName     string `json:"displayName"`
	Value           string `json:"value"`
	Unit            string `json:"unit"`
	DeviceID        int    `json:"deviceId"`
	HubID           int    `json:"hubId"`
	InstalledAppID  int    `json:"installedAppId"`
	DescriptionText string `json:"descriptionText"`
}

type HubLog struct {
	Name  string `json:"name"`
	Msg   string `json:"msg"`
	ID    int    `json:"id"`
	Time  string `json:"time"`
	Type  string `json:"type"`
	Level string `json:"level"`
}

type Job struct {
	id      int
	feedurl string
}

type Result struct {
	job  Job
	msg  HubMessage
	emsg HubEvent
	lmsg HubLog
}

var (
	slogfilepath = "log/system.log"
	llogfilepath = "log/hublogs.log"
	elogfilepath = "log/hubevents.log"
	slogfile     *os.File
	//slogfile, _ = os.OpenFile(slogfilepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	llogfile *os.File
	//llogfile, _ = os.OpenFile(llogfilepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	elogfile *os.File
	//elogfile, _ = os.OpenFile(elogfilepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	slg *log.Logger
	//slg = *log.New(slogfile, "", log.LUTC|log.Ldate|log.Ltime|log.Lshortfile)
	llg *log.Logger
	//llg = *log.New(llogfile, "", log.LUTC|log.Ldate|log.Ltime|log.Lshortfile)
	elg *log.Logger
	//elg = *log.New(elogfile, "", log.LUTC|log.Ldate|log.Ltime|log.Lshortfile)
	//	jobs    = make(chan Job, 10)
//	results = make(chan Result, 10)
//	errch   = make(chan error, 10)
)

func init() {
	slogfile, _ = os.OpenFile(slogfilepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	//defer slogfile.Close()
	slg = log.New(slogfile, "", log.LUTC|log.Ldate|log.Ltime|log.Lshortfile)
	llogfile, _ = os.OpenFile(llogfilepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	//defer llogfile.Close()
	llg = log.New(llogfile, "", log.LUTC|log.Ldate|log.Ltime|log.Lshortfile)
	elogfile, _ = os.OpenFile(elogfilepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	//defer elogfile.Close()
	elg = log.New(elogfile, "", log.LUTC|log.Ldate|log.Ltime|log.Lshortfile)
}

func createWorkerPool(noOfWorkers int, jobs *chan Job) {
	var wg sync.WaitGroup
	done := make(chan bool)
	results := make(chan Result, 10)
	go result(done, &results)
	for i := 0; i < noOfWorkers; i++ {
		wg.Add(1)
		go worker(&wg, jobs, &results)
	}
	wg.Wait()
	<-done
	close(results)
}

func allocate(j []Job, jobs *chan Job) {
	for _, job := range j {
		*jobs <- job
	}
	close(*jobs)
}

func retry(j *Job, d *ws.Dialer, rc int, ri time.Duration) (c *ws.Conn, err error) {
	fmt.Println("retry: retry", rc, "times every", ri*time.Second, "seconds:", j.feedurl)
	slg.Println("retry: retry", rc, "times every", ri*time.Second, "seconds:", j.feedurl)
	count := 0
	if rc == 0 {
		//count := 0
		for {
			time.Sleep(ri * time.Second)
			fmt.Println("retry: retry count:", count, j.feedurl)
			slg.Println("retry: retry count:", count, j.feedurl)
			c, _, err = d.Dial(j.feedurl, nil)
			if err != nil {
				fmt.Println("retry: retry error:", count, j.feedurl, err)
				slg.Println("retry: retry error:", count, j.feedurl, err)
				count += 1
				continue
			}
			go keepAlive(c, 20*time.Second)
			break
		}
	} else if rc > 0 {
		//count := 0
		for i := 0; i < rc; i++ {
			time.Sleep(ri * time.Second)
			fmt.Println("retry: retry iteration:", i, "of", rc, j.feedurl)
			slg.Println("retry: retry iteration:", i, "of", rc, j.feedurl)
			c, _, err = d.Dial(j.feedurl, nil)
			if err != nil {
				fmt.Println("retry: retry error:", count, j.feedurl, err)
				slg.Println("retry: retry error:", count, j.feedurl, err)
				if i == rc-1 {
					return nil, err
				} else {
					count += 1
					continue
				}
			}
			go keepAlive(c, 20*time.Second)
			break
		}
	}
	fmt.Println("retry: successfully reconnected after:", count, j.feedurl, err)
	slg.Println("retry: successfully reconnected after:", count, j.feedurl, err)
	time.Sleep(1 * time.Second)
	return c, nil
}

func keepAlive(c *ws.Conn, timeout time.Duration) {
	fmt.Println("keepAlive: Starting keepAlive for", c.LocalAddr(), c.RemoteAddr())
	lastResponse := time.Now()
	c.SetPongHandler(func(msg string) error {
		lastResponse = time.Now()
		fmt.Println("keepAlive: received ws.PongMessage for", msg, lastResponse, c.LocalAddr(), c.RemoteAddr())
		return nil
	})

	go func() {
		for {
			fmt.Println("keepAlive: sending ws.PingMessage to connection", c.LocalAddr(), c.RemoteAddr(), time.Since(lastResponse))
			err := c.WriteMessage(ws.PingMessage, []byte("keepalive"))
			if err != nil {
				fmt.Printf("keepAlive: Sending ping message returned error %s %s\n", err, c.LocalAddr())
				return
			}
			time.Sleep(timeout / 2)
			if time.Since(lastResponse) > timeout {
				fmt.Printf("keepAlive: Ping don't get response, disconnecting to %s\n", c.LocalAddr())
				_ = c.Close()
				return
			}
		}
	}()
}

func worker(wg *sync.WaitGroup, jobs *chan Job, results *chan Result) {
	var wsDialer ws.Dialer
	msg := HubMessage{}
	emsg := HubEvent{}
	lmsg := HubLog{}
	for job := range *jobs {
		wsConn, _, err := wsDialer.Dial(job.feedurl, nil)
		if err != nil {
			fmt.Println("worker: error dialing:", err, job.feedurl)
			slg.Println("worker: error dialing:", err, job.feedurl)
			for i := 0; i < 5; i++ {
				fmt.Println("worker: sleeping 60 seconds", i)
				slg.Println("worker: sleeping 60 seconds", i)
				time.Sleep(10 * time.Second)
				fmt.Println("worker: retry iteration:", i, "of", 5, job.feedurl)
				slg.Println("worker: retry iteration:", i, "of", 5, job.feedurl)
				wsConn, _, err = wsDialer.Dial(job.feedurl, nil)
				if err != nil {
					fmt.Println("worker: retry error:", i, job.feedurl, err)
					slg.Println("worker: retry error:", i, job.feedurl, err)
					if i == 4 {
						fmt.Println("worker: error retrying and giving up:", err, job.feedurl)
						slg.Println("worker: error retrying and giving up:", err, job.feedurl)
						//close(*jobs)
						//close(*results)
						wg.Done()
						return
					} else {
						continue
					}
				}
				fmt.Println("retry: successfully reconnected after:", i, job.feedurl, err)
				slg.Println("retry: successfully reconnected after:", i, job.feedurl, err)
				time.Sleep(1 * time.Second)
			}
			//				if wsConn, err = retry(&job, &wsDialer, 5, 60); err != nil {
			//close(*jobs)
			//wg.Done()
			//				break
			//			}
			//*jobs <- job
			//continue
		}
		go keepAlive(wsConn, 20*time.Second)
		fmt.Println("worker: Connected to", job.id, job.feedurl, wsConn.RemoteAddr(), wsConn.LocalAddr())
		slg.Println("worker: Connected to", job.id, job.feedurl, wsConn.RemoteAddr(), wsConn.LocalAddr())
		defer wsConn.Close()
		var out Result
		for {
			count := 0
			if strings.Contains(job.feedurl, "eventsocket") {
				if err := wsConn.ReadJSON(&emsg); err != nil {
					fmt.Println("worker: error ReadJSON:", err, job.feedurl)
					slg.Println("worker: error ReadJSON:", err, job.feedurl)
					for {
						time.Sleep(30 * time.Second)
						fmt.Println("worker: retry count:", count, job.feedurl)
						slg.Println("worker: retry count:", count, job.feedurl)
						wsConn, _, err = wsDialer.Dial(job.feedurl, nil)
						if err != nil {
							fmt.Println("worker: retry error:", count, job.feedurl, err)
							slg.Println("worker: retry error:", count, job.feedurl, err)
							count += 1
							if count == 4 {
								break
							}
							continue
						}
						fmt.Println("worker: successfully reconnected after:", count, job.feedurl, err)
						slg.Println("worker: successfully reconnected after:", count, job.feedurl, err)
						time.Sleep(1 * time.Second)
						go keepAlive(wsConn, 20*time.Second)
						break
					}
					//if ws.IsUnexpectedCloseError(err, ws.CloseGoingAway) || errors.Is(err, syscall.ECONNRESET) {
					//if wsConn, err = retry(&job, &wsDialer, 0, 60); err != nil {
					//	fmt.Println("worker: error retrying and giving up:", err, job.feedurl)
					//	slg.Println("worker: error retrying and giving up:", err, job.feedurl)
					//close(*jobs)
					//wg.Done()
					//	break
					//}
					//}
				}
				//out = Result{job, HubMessage{}, emsg, HubLog{}}
			} else if strings.Contains(job.feedurl, "logsocket") {
				if wsConn != nil {
					if err := wsConn.ReadJSON(&lmsg); err != nil {
						fmt.Println("worker: error ReadJSON:", err, job.feedurl)
						slg.Println("worker: error ReadJSON:", err, job.feedurl)
						//if ws.IsUnexpectedCloseError(err, ws.CloseGoingAway) || errors.Is(err, syscall.ECONNRESET) {
						for {
							time.Sleep(30 * time.Second)
							fmt.Println("worker: retry count:", count, job.feedurl)
							slg.Println("worker: retry count:", count, job.feedurl)
							wsConn, _, err = wsDialer.Dial(job.feedurl, nil)
							if err != nil {
								fmt.Println("worker: retry error:", count, job.feedurl, err)
								slg.Println("worker: retry error:", count, job.feedurl, err)
								count += 1
								if count == 4 {
									break
								}
								continue
							}
							fmt.Println("worker: successfully reconnected after:", count, job.feedurl, err)
							slg.Println("worker: successfully reconnected after:", count, job.feedurl, err)
							time.Sleep(1 * time.Second)
							go keepAlive(wsConn, 20*time.Second)
							break
						}
						//if wsConn, err = retry(&job, &wsDialer, 0, 60); err != nil {
						//	fmt.Println("worker: error retrying and giving up:", err, job.feedurl)
						//	slg.Println("worker: error retrying and giving up:", err, job.feedurl)
						//close(*jobs)
						//wg.Done()
						//	break
						//}
						//}
					}
				}
				//out = Result{job, HubMessage{}, HubEvent{}, lmsg}
			} else {
				if err := wsConn.ReadJSON(&msg); err != nil {
					fmt.Println("worker: error ReadJSON:", err, job.feedurl)
					slg.Println("worker: error ReadJSON:", err, job.feedurl)
					//if ws.IsUnexpectedCloseError(err, ws.CloseGoingAway) || errors.Is(err, syscall.ECONNRESET) {
					//*errch <- err
					if wsConn, err = retry(&job, &wsDialer, 0, 60); err != nil {
						fmt.Println("worker: error retrying and giving up:", err, job.feedurl)
						slg.Println("worker: error retrying and giving up:", err, job.feedurl)
						//close(*jobs)
						//wg.Done()
						break
					}
					//}
				}
				//out = Result{job, msg, HubEvent{}, HubLog{}}
			}
			out = Result{job, msg, emsg, lmsg}
			*results <- out
			continue
		}
	}
	wg.Done()
}

func handleError(errordone chan bool, errch *chan error) {
	for e := range *errch {
		fmt.Println("handleError: websocket errored out:", e)
		fmt.Println("handleError: Sleeping 30 and retrying 5 times:")
		//close(jobs)
		//close(results)
		for i := 0; i < 5; i++ {
			fmt.Println("handleError: iteration:", i)
			time.Sleep(30 * time.Second)
			if err := startConn(); err != nil {
				fmt.Println("handleError: error connecting", err)
				continue
			}
		}
		fmt.Println("handleError: retry 5 times to no avail")
		errordone <- true
	}
}

func startConn() error {
	var (
		jobs = make(chan Job, 10)
		//results = make(chan Result, 10)
		//errch   = make(chan error, 10)
	)
	j := []Job{}
	j1 := Job{
		id:      0,
		feedurl: "wss://madenhub01-office.things.denn.com/eventsocket",
	}
	j2 := Job{
		id:      1,
		feedurl: "wss://madenhub01-office.things.denn.com/logsocket",
	}
	j = append(j, j1, j2)
	go allocate(j, &jobs)
	//done := make(chan bool)
	//	errordone := make(chan bool)
	//go result(done, &results)
	//	go handleError(errordone, &errch)
	createWorkerPool(2, &jobs)
	//<-errordone
	//close(results)
	//<-done
	//close(jobs)
	return errors.New("startConn: connections terminated")
}

func result(done chan bool, results *chan Result) {
	for result := range *results {
		var m []byte
		if strings.Contains(result.job.feedurl, "eventsocket") {
			m, _ = json.MarshalIndent(result.emsg, "", "\t")
			elg.Println("result results: ", result.job.id, result.job.feedurl, "\nMessage:\n", string(m))
		} else if strings.Contains(result.job.feedurl, "logsocket") {
			m, _ = json.MarshalIndent(result.lmsg, "", "\t")
			llg.Println("result results: ", result.job.id, result.job.feedurl, "\nMessage:\n", string(m))
		} else {
			m, _ = json.MarshalIndent(result.msg, "", "\t")
		}
		fmt.Println("result results: ", result.job.id, result.job.feedurl, "\nMessage:\n", string(m))
	}
	done <- true
}

func main() {
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)

	go func() {
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	/*
		slogfile, _ = os.OpenFile(slogfilepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		defer slogfile.Close()
		slg = log.New(slogfile, "", log.LUTC|log.Ldate|log.Ltime|log.Lshortfile)
		llogfile, _ = os.OpenFile(llogfilepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		defer llogfile.Close()
		llg = log.New(llogfile, "", log.LUTC|log.Ldate|log.Ltime|log.Lshortfile)
		elogfile, _ = os.OpenFile(elogfilepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		defer elogfile.Close()
		elg = log.New(elogfile, "", log.LUTC|log.Ldate|log.Ltime|log.Lshortfile)
	*/
	slg.Println("main: starting wshubitat:")
	if err := startConn(); err != nil {
		fmt.Println("main: Finished connections:", err)
		slg.Println("main: Finished connections:", err)
	}
	/*
		j := []Job{}
		j1 := Job{
			id:      0,
			feedurl: "ws://10.1.3.130/eventsocket",
		}
		j2 := Job{
			id:      1,
			feedurl: "ws://10.1.3.130/logsocket",
		}
		j = append(j, j1, j2)
		go allocate(j)
		done := make(chan bool)
		go result(done)
		go handleError()
		createWorkerPool(2)
		<-done
	*/
}

/*
func main() {
	out = true
	var wsDialer ws.Dialer
	ewsConn, _, err := wsDialer.Dial("ws://10.1.3.130/eventsocket", nil)
	if err != nil {
		println(err.Error())
	}
	fmt.Println("Connected to eventsocket:", ewsConn.LocalAddr().String(), ewsConn.RemoteAddr().String())

	lwsConn, _, err := wsDialer.Dial("ws://10.1.3.130/logsocket", nil)
	if err != nil {
		println(err.Error())
	}
	fmt.Println("Connected to logsocket:", lwsConn.LocalAddr().String(), lwsConn.RemoteAddr().String())

	//if err := wsConn.WriteJSON(""); err != nil {
	//	println(err.Error())
	//}

	for {
		lmsg := HubLog{}
		if err := lwsConn.ReadJSON(&lmsg); err != nil {
			fmt.Println(err)
			break
		}
		//_, lmsg, err := lwsConn.ReadMessage()
		//if err != nil {
		//	fmt.Println(err)
		//	break
		//}
		emsg := HubEvent{}
		if err := ewsConn.ReadJSON(&emsg); err != nil {
			fmt.Println(err)
			break
		}
		//s := string(msg)
		jl, err := json.Marshal(lmsg)
		if err != nil {
			fmt.Println(err)
			break
		}
		je, err := json.Marshal(emsg)
		if err != nil {
			fmt.Println(err)
			break
		}
		if out {
			color.Cyan.Println("Event: ", string(je))
			color.Green.Println("Log: ", string(jl))
		}
	}
}
*/
//func doIngest()
