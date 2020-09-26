package main

import (
	"github.io/Liqo/JobProfiler/internal/profiling"
	"log"
	"os"
)

func main() {
	os.Setenv("PROMETHEUS_URL", "172.18.0.2")
	os.Setenv("PROMETHEUS_PORT", "32041")
	os.Setenv("PROFILING_NAMESPACE", "")
	os.Setenv("BACKGROUND_ROUTINE_UPDATE_TIME", "60")
	os.Setenv("BACKGROUND_ROUTINE_ENABLED", "FALSE")
	os.Setenv("TIMESLOTS", "4")

	var profiling profiling.ProfilingSystem

	if err := profiling.Init(); err != nil {
		log.Print(err)
	}

	if err := profiling.StartProfiling(os.Getenv("PROFILING_NAMESPACE")); err != nil {
		log.Print(err)
	}

	log.Print("-- Execution terminated --")
}
