package main

import (
	"bufio"
	"encoding/csv"
	"encoding/gob"
	"log"
	"net"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"time"
)

type TimeSeries struct {
	StartTime int64
	Duration  float64
	Addr	  string
}

func initListener(spinnerURL string, loc string, ip string) {
	listener, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Println(err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
		}
		go recv(conn, spinnerURL, loc, ip)
	}
}

func startSender(addr string, sendChan chan float64) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Println(err)
		return
	}
	encoder := gob.NewEncoder(conn)
	defer conn.Close()

	for {
		select {
		case msg := <- sendChan:
			err := encoder.Encode(&msg)
			if err != nil {
				log.Println(err)
				return
			}
		}
	}
}

func recv(conn net.Conn, spinnerURL string, loc string, ip string) {
	for {
		decoder := gob.NewDecoder(conn)
		var duration float64
		err := decoder.Decode(&duration)
		if err != nil {
			log.Println(err)
			return
		}
		if duration > 0 {
			captainCMD := "docker run -d --rm -v /var/run/docker.sock:/var/run/docker.sock armadaumn/captainaws server " + loc + " keller " + spinnerURL +" " + ip
			//cmd := exec.Command("docker", "run", "-it", "--rm", "-v", "/var/run/docker.sock:/var/run/docker.sock",
			//	"--net", "spinner-local-network", "armadaumn/captain", "server", "close", "keller", "spinner:5912")
			cmd := exec.Command("/bin/sh", "-c", captainCMD)
			stdout, err := cmd.Output()
			log.Println(string(stdout))
			if err != nil {
				log.Println(err)
			}
			//var p = new Process { StartInfo = new ProcessStartInfo(
			go handleTimeout(duration)
		} else {
			// cmd := exec.Command("docker", "kill", "$(docker", "ps", "-q)")
			cmd := exec.Command("/bin/sh", "-c", "docker kill $(docker ps -q)")
			_, err := cmd.Output()
			if err != nil {
				log.Println(err)
			} else {
				log.Println("Containers are removed")
			}
		}
	}
}

func handleTimeout(duration float64) {
	select {
	case <- time.After(time.Duration(duration) * time.Second):
		cmd := exec.Command("/bin/sh", "-c", "docker kill $(docker ps -q)")
			_, err := cmd.Output()
			if err != nil {
				log.Println(err)
			} else {
				log.Println("Containers are removed")
			}
	}
}

func readCsv() ([]TimeSeries, error) {
	file, err := os.Open("./devicesim.csv")
	if err != nil {
		log.Println(err)
		return nil, err
	}
	lines, err := csv.NewReader(file).ReadAll()
	if err != nil {
		log.Println(err)
		return nil, err
	}

	addrFile, err := os.Open("./addr.csv")
	if err != nil {
		log.Println(err)
		return nil, err
	}
	addrLines, err := csv.NewReader(addrFile).ReadAll()
	if err != nil {
		log.Println(err)
		return nil, err
	}

	timeSeries := make([]TimeSeries, 0)

	for i, line := range lines {
		startTime, err := strconv.ParseInt(line[0], 10, 64)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		duration, err := strconv.ParseFloat(line[1], 64)
		if err != nil {
			log.Println(err)
			return nil, err
		}

		timeSeries = append(timeSeries, TimeSeries{
			StartTime: startTime,
			Duration:  duration,
			Addr: 	   addrLines[i][0],
		})
	}
	return timeSeries, nil
}

func main() {
	argc := len(os.Args)
	if argc > 1 {
		if argc != 4 {
			log.Println("Need [spinnerIp:port] [location] [self ip]")
			return
		}
		//Start listener mode on captain nodes
		initListener(os.Args[1], os.Args[2], os.Args[3])
		return
	}

	timeSeries, err := readCsv()
	if err != nil {
		return
	}

	// Start sender mode on central controller unit
	sendChans := make([]chan float64, len(timeSeries))
	for i := 0; i < len(sendChans); i++ {
		sendChans[i] = make(chan float64)
		go startSender(timeSeries[i].Addr, sendChans[i])
	}

	sort.Slice(timeSeries, func(i, j int) bool {
		return timeSeries[i].StartTime < timeSeries[j].StartTime
	})

	scanner := bufio.NewScanner(os.Stdin)
	timers := make([]*time.Timer, len(timeSeries))

	for {
		for i := 0; i < len(timers); i++ {
			timers[i] = time.NewTimer(time.Duration(timeSeries[i].StartTime) * time.Second)
		}

		t := time.Now()
		for i := 0; i < len(timers); i++ {
			select {
			case <-timers[i].C:
				log.Println(time.Now().Sub(t))
				sendChans[i] <- timeSeries[i].Duration
				log.Printf("start cmd sent to node %d\n", i)
				//go handleTimeout(timeSeries[i].Duration, sendChans[i])
			}
		}

		isTerminated := false
		for scanner.Scan() {
			cmd := scanner.Text()
			if cmd == "exit" {
				log.Println("start terminating")
				for _, sendChan := range sendChans {
					sendChan <- -1
					isTerminated = true
				}
			} else if cmd == "start" {
				if !isTerminated {
					log.Println("All captains are still running")
					continue
				}
				log.Println("restarting ...")
				break
			}
		}
	}
}

