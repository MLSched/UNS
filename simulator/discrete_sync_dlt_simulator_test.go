package simulator

import (
	"fmt"
	"log"
	"os"
	"path"
	"testing"
	"time"
)

var syncConfigPath = "/Users/purchaser/go/src/UNS/cases/sync_simulator_configuration.json"
var logDir = "/Users/purchaser/go/src/UNS/logs"
var enableLog = false

func init() {
	if enableLog {
		fp, err := os.OpenFile(path.Join(logDir, fmt.Sprintf("simulator_%s.log", time.Now().Format("2006-01-02_15:04:05"))), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			panic(err)
		}
		log.SetOutput(fp)
		log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
		log.Printf("test")
	}
}

func TestSimulator(t *testing.T) {
	simulator := NewDiscreteSyncDLTSimulator(syncConfigPath)
	simulator.StartSimulation()
}
