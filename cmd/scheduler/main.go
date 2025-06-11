package main

import (
	"flag"
	"log"

	"github.com/saurabhsingh121/distributed-scheduler/pkg/common"
	"github.com/saurabhsingh121/distributed-scheduler/pkg/scheduler"
)

var (
	schedulerPort = flag.String("scheduler-port", ":8081", "Port for the scheduler server")
)

func main() {
	dbConnectionString := common.GetDBConnectionString()
	schedulerServer := scheduler.NewServer(*schedulerPort, dbConnectionString)
	err := schedulerServer.Start()
	if err != nil {
		log.Fatalf("Failed to start scheduler server: %v", err)
	}
}
