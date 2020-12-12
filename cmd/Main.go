package main

import (
	"crownlabs.com/profiling/internal/profiling"
	"log"
	"os"
)

func main() {

	var profiling profiling.ProfilingSystem

	if err := profiling.Init(); err != nil {
		log.Print(err)
	}

	if err := profiling.StartProfiling(os.Getenv("PROFILING_NAMESPACE")); err != nil {
		log.Print(err)
	}

	log.Print("-- Execution terminated --")
}
