package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type MeasurementData struct {
	min   float32
	max   float32
	count int32
	mean  float32
	sum   float32
}

const CHUNK_SIZE = 5000

func main() {
	file, _ := os.Open("../../../../measurements.txt")
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// Buffer to hold lines
	var buffer []string
	resultMap := make(map[string]*MeasurementData)
	start := time.Now()
	for scanner.Scan() {
		buffer = append(buffer, scanner.Text())
		if len(buffer) >= CHUNK_SIZE {
			for _, line := range buffer {
				parts, err := splitString(line)
				if err != nil {
					fmt.Println("Error splitting line:", err)
					return
				}
				key := parts[0]
				val := parts[1]
				parsedValue, err := strconv.ParseFloat(val, 32)
				if err != nil {
					fmt.Println("Error converting value to float:", err)
					return
				}
				updateMeasurementData(resultMap, key, parsedValue)
			}
			// Clear the buffer
			buffer = buffer[:0]
		}
	}
	// Process any remaining lines in the buffer
	if len(buffer) > 0 {
		fmt.Printf("Processing %d lines\n", len(buffer))
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}

	for _, data := range resultMap {
		data.mean = data.sum / float32(data.count)
	}

	elapsed := time.Since(start)
	// printMap(resultMap)
	handleStdOut(resultMap)
	fmt.Printf("File read in %s\n", elapsed)
}


func splitString(line string) ([]string, error) {
	parts := strings.Split(line, ";")
	if len(parts) != 2 {
		return nil, errors.New("line does not contain exactly one semicolon")
	}
	return parts, nil
}

func updateMeasurementData(resultMap map[string]*MeasurementData, key string, value float64) {

	if _, exists := resultMap[key]; !exists {
		resultMap[key] = &MeasurementData{
			min:   float32(value),
			max:   float32(value),
			count: 0,
			mean:  0,
		}
	}

	data := resultMap[key]
	data.count += 1
	data.sum += float32(value)
	if float32(value) < data.min {
		data.min = float32(value)
	}
	if float32(value) > data.max {
		data.max = float32(value)
	}
}

func handleStdOut(resultMap map[string]*MeasurementData) {
	keys := make([]string, 0, len(resultMap))
	for key := range resultMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var output strings.Builder
	output.WriteString("{")
	for i, key := range keys {
		data := resultMap[key]
		output.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", key, data.min, data.mean, data.max))
		if i < len(keys)-1 {
			output.WriteString(", ")
		}
	}
	output.WriteString("}")
	fmt.Println(output.String())
}
