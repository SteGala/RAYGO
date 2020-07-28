package main

import (
	"github.io/SteGala/JobProfiler/src/profiling"
	"log"
	"os"
)

func main() {

	var profiling profiling.ProfilingSystem

	if err := profiling.Init(); err != nil {
		log.Print(err)
	}

	if err := profiling.StartProfile(os.Getenv("PROFILING_NAMESPACE")); err != nil {
		log.Print(err)
	}

	log.Print("-- Execution terminated --")
}
