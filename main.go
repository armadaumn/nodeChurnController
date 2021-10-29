package main

import (
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	// Check input
	if len(os.Args) > 1 {
		if os.Args[1] == "client" {
			// Listen on Client operation
			if len(os.Args) != 7 {
				log.Println("Need [client] [appManager IP] [appManager port] [location] [tag] [TopN]")
				return
			}
			initClientListener(os.Args[2], os.Args[3], os.Args[4], os.Args[5], os.Args[6])
		} else if os.Args[1] == "captain" {
			// Listen on Captain operation
			if len(os.Args) != 5 {
				log.Println("Need [captain] [spinnerIp:port] [location] [self ip]")
				return
			}
			initListener(os.Args[2], os.Args[3], os.Args[4])
		} else if os.Args[1] == "control" {
			// This is central controller
			if len(os.Args) != 4 {
				log.Println("Need [control] [whenStartClient] [clientDuration]")
				return
			}
			// convert whenStartClient int, clientDuration int
			whenStartClient, err := strconv.Atoi(os.Args[2])
			if err != nil {
				log.Println("Wrong input format: Need [control] [whenStartClient] [clientDuration]")
				return
			}
			clientDuration, err := strconv.Atoi(os.Args[3])
			if err != nil {
				log.Println("Wrong input format: Need [control] [whenStartClient] [clientDuration]")
				return
			}
			runCentralController(whenStartClient, clientDuration)
		} else {
			log.Println("First argument wrong [control, captain or client]")
			return
		}
		return
	} else {
		log.Println("Need [control, captain or client] ... ... ")
		return
	}
}

/////////////////////// (1) Central controller functions ///////////////////////

type TimeSeries struct {
	StartTime int64
	Duration  float64
	Addr      string
}

func runCentralController(whenStartClient int, clientDuration int) {
	// Capture the signal
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// This is central controller: read captain address and node configuration
	timeSeries, err := readCsv()
	if err != nil {
		return
	}

	// Start a seperate routine for each captain on the profile
	// Each routine waits at the channel for start command
	sendChans := make([]chan float64, len(timeSeries))
	for i := 0; i < len(sendChans); i++ {
		sendChans[i] = make(chan float64)
		// Only pass the captain ip and the waiting channel
		go startSender(timeSeries[i].Addr, sendChans[i])
	}

	// Construct the interval list for sleep function
	intervals := make([]time.Duration, len(timeSeries))
	lastTimeStamp := int64(0)
	for i := 0; i < len(intervals); i++ {
		intervals[i] = time.Duration(timeSeries[i].StartTime-int64(lastTimeStamp)) * time.Second
		lastTimeStamp = timeSeries[i].StartTime
	}

	// Read all client addresses
	clients := readClientCSV()
	// Start all clients
	go runClientController(clients, whenStartClient, clientDuration)
	// Start sending the start command
	log.Println("Start the procedure ...")
	t1 := time.Now()
	for i := 0; i < len(intervals); i++ {
		time.Sleep(intervals[i])
		sendChans[i] <- timeSeries[i].Duration
		timeElapsed := time.Since(t1)
		fmt.Print("Timestamp %v: start cmd sent to node %d\n", timeElapsed, i)
	}

	// Wait for exit command
	<-signalChan
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

	addrFile, err := os.Open("./captain.csv")
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
			Addr:      addrLines[i][0],
		})
	}
	return timeSeries, nil
}

func startSender(addr string, sendChan chan float64) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Println(err)
		return
	}
	encoder := gob.NewEncoder(conn)
	defer conn.Close()
	// Waiting for the start command
	msg := <-sendChan
	err = encoder.Encode(&msg)
	if err != nil {
		log.Println(err)
		return
	}
}

func readClientCSV() []string {
	// Read all client addresses
	addresses := make([]string, 0)
	addrFile, err := os.Open("./client.csv")
	if err != nil {
		log.Println(err)
		os.Exit(0)
	}
	addrLines, err := csv.NewReader(addrFile).ReadAll()
	if err != nil {
		log.Println(err)
		os.Exit(0)
	}
	fmt.Println(addrLines)
	for i := 0; i < len(addrLines); i++ {
		addresses = append(addresses, addrLines[i][0])
	}
	return addresses
}

func runClientController(clients []string, start int, duration int) {

	// Wait for start time
	time.Sleep(time.Duration(start) * time.Second)
	// Start all clients
	for i := 0; i < len(clients); i++ {
		conn, err := net.Dial("tcp", clients[i])
		if err != nil {
			log.Println(err)
			os.Exit(0)
		}
		encoder := gob.NewEncoder(conn)
		defer conn.Close()
		// Send stat command to this client
		err = encoder.Encode(1)
		if err != nil {
			log.Println(err)
			os.Exit(0)
		}
	}

	// Wait for experiment duration
	time.Sleep(time.Duration(duration) * time.Second)
	// Stop all clients
	for i := 0; i < len(clients); i++ {
		conn, err := net.Dial("tcp", clients[i])
		if err != nil {
			log.Println(err)
			os.Exit(0)
		}
		encoder := gob.NewEncoder(conn)
		defer conn.Close()
		// Send stat command to this client
		err = encoder.Encode(-1)
		if err != nil {
			log.Println(err)
			os.Exit(0)
		}
	}
}

/////////////////////// (2) Captain controller functions ///////////////////////

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

func recv(conn net.Conn, spinnerURL string, loc string, ip string) {
	decoder := gob.NewDecoder(conn)
	var duration float64
	err := decoder.Decode(&duration)
	if err != nil {
		log.Println(err)
		return
	}
	if duration > 0 {
		captainCMD := "docker run -d --rm -v /var/run/docker.sock:/var/run/docker.sock armadaumn/captainaws server " + loc + " keller " + spinnerURL + " " + ip
		cmd := exec.Command("/bin/sh", "-c", captainCMD)
		stdout, err := cmd.Output()
		log.Println(string(stdout))
		if err != nil {
			log.Println(err)
		}
		// Wait for the given lifetime and stop the captain
		time.Sleep(time.Duration(duration) * time.Second)
		cmd = exec.Command("/bin/sh", "-c", "docker kill $(docker ps -q)")
		_, err = cmd.Output()
		if err != nil {
			log.Println(err)
		} else {
			log.Println("Containers are removed")
		}
	} else {
		cmd := exec.Command("/bin/sh", "-c", "docker kill $(docker ps -q)")
		_, err := cmd.Output()
		if err != nil {
			log.Println(err)
		} else {
			log.Println("Containers are removed")
		}
	}
}

/////////////////////// (3) Client controller functions ///////////////////////

func initClientListener(ip, port, location, tag, topN string) {
	listener, err := net.Listen("tcp", ":8001")
	if err != nil {
		log.Println(err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
		}
		go clientHandler(conn, ip, port, location, tag, topN)
	}
}

func clientHandler(conn net.Conn, ip, port, location, tag, topN string) {
	decoder := gob.NewDecoder(conn)
	var cmd int
	err := decoder.Decode(&cmd)
	if err != nil {
		log.Println(err)
		return
	}
	// Note: cmd here works as start (1) or stop (-1)
	if cmd > 0 {
		// This is the start command
		captainCMD := "docker run --rm armadaumn/objectdetectionclient2.0 " + ip + " " + port + " " + location + " " + tag + " " + topN
		cmd := exec.Command("/bin/sh", "-c", captainCMD)
		stdout, err := cmd.Output()
		log.Println(string(stdout))
		if err != nil {
			log.Println(err)
		}
	} else {
		// This is the stop command
		cmd := exec.Command("/bin/sh", "-c", "docker kill $(docker ps -q)")
		_, err := cmd.Output()
		if err != nil {
			log.Println(err)
		} else {
			log.Println("Client is stopped")
		}
	}
}
