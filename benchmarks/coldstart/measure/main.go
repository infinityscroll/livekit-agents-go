// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"time"
)

type report struct {
	Samples   int     `json:"samples"`
	Warmups   int     `json:"warmups"`
	MeanNS    int64   `json:"mean_ns"`
	MinNS     int64   `json:"min_ns"`
	P50NS     int64   `json:"p50_ns"`
	P95NS     int64   `json:"p95_ns"`
	P99NS     int64   `json:"p99_ns"`
	MaxNS     int64   `json:"max_ns"`
	SamplesNS []int64 `json:"samples_ns"`
}

func main() {
	binary := flag.String("binary", "", "freshly built probe executable")
	samples := flag.Int("samples", 100, "number of fresh processes")
	warmups := flag.Int("warmups", 5, "unreported filesystem-cache warmups")
	jsonOutput := flag.Bool("json", false, "emit machine-readable output including every sample")
	flag.Parse()
	if *binary == "" || *samples < 1 || *warmups < 0 {
		fmt.Fprintln(os.Stderr, "usage: measure -binary PATH [-samples 100] [-warmups 5]")
		os.Exit(2)
	}
	for range *warmups {
		if err := run(*binary); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	values := make([]time.Duration, *samples)
	for index := range values {
		started := time.Now()
		if err := run(*binary); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		values[index] = time.Since(started)
	}
	result := summarize(values, *warmups)
	if *jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(result); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	fmt.Printf("samples=%d mean=%s min=%s p50=%s p95=%s p99=%s max=%s\n",
		result.Samples, time.Duration(result.MeanNS), time.Duration(result.MinNS),
		time.Duration(result.P50NS), time.Duration(result.P95NS),
		time.Duration(result.P99NS), time.Duration(result.MaxNS))
}

func summarize(values []time.Duration, warmups int) report {
	ordered := slices.Clone(values)
	slices.Sort(ordered)
	var total time.Duration
	raw := make([]int64, len(values))
	for index, value := range values {
		total += value
		raw[index] = int64(value)
	}
	return report{
		Samples:   len(values),
		Warmups:   warmups,
		MeanNS:    int64(total / time.Duration(len(values))),
		MinNS:     int64(ordered[0]),
		P50NS:     int64(percentile(ordered, 50)),
		P95NS:     int64(percentile(ordered, 95)),
		P99NS:     int64(percentile(ordered, 99)),
		MaxNS:     int64(ordered[len(ordered)-1]),
		SamplesNS: raw,
	}
}

func run(binary string) error {
	command := exec.Command(binary)
	command.Stdout, command.Stderr = io.Discard, io.Discard
	if err := command.Run(); err != nil {
		return fmt.Errorf("run %s: %w", binary, err)
	}
	return nil
}

func percentile(values []time.Duration, percentage int) time.Duration {
	index := (len(values)*percentage + 99) / 100
	if index < 1 {
		index = 1
	}
	return values[index-1]
}
