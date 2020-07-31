package main

import (
	"log"
	"os"

	"github.io/SteGala/JobProfiler/pkg/profiling"
)

func main() {

	var profiling profiling.ProfilingSystem

	if err := profiling.Init(); err != nil {
		log.Print(err)
		return
	}

	if err := profiling.StartProfile(os.Getenv("PROFILING_NAMESPACE")); err != nil {
		log.Print(err)
	}

	log.Print("-- Execution terminated --")
}
