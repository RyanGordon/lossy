package lossy

import (
	"io/ioutil"
	"strconv"
	"strings"
)

func readSaturatorTrace(path string) (map[int]int, int64, error) {
	// key is the millisecond a packet can be pulled from the queue and value is how many can be pulled
	// packets can be up to 1500 bytes in size

	if path == "" {
		return map[int]int{}, 0, nil
	}

	traceStr, err := ioutil.ReadFile(path)
	if err != nil {
		return map[int]int{}, 0, err
	}

	trace := make(map[int]int)
	traceArr := strings.Split(string(traceStr), "\n")
	maxMs := 0
	for _, val := range traceArr {
		if val == "" {
			continue
		}

		ms, err := strconv.Atoi(val)
		if err != nil {
			return map[int]int{}, 0, err
		}

		trace[ms]++

		if ms > maxMs {
			maxMs = ms
		}
	}

	return trace, int64(maxMs), err
}
