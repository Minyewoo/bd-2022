package main

import (
	"bufio"
	"container/list"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

func getFactorsCount(number int, counts chan<- int) {
	simpleCandidate := 2
	simpleDividers := list.New()

	for simpleCandidate*simpleCandidate <= number {
		for number%simpleCandidate == 0 {
			simpleDividers.PushBack(simpleCandidate)
			number /= simpleCandidate
		}
		simpleCandidate += 1
	}

	if number > 1 {
		simpleDividers.PushBack(number)
	}

	counts <- simpleDividers.Len()
}

func summarizeCounts(counts <-chan int, sum chan<- int) {
	factorsCountSum := 0
	for count := range counts {
		factorsCountSum += count
	}

	sum <- factorsCountSum
}

func main() {
	fileName := "data"
	counts := make(chan int)

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	nums_count := 0
	startTime := time.Now()
	for scanner.Scan() {
		num, err := strconv.Atoi(scanner.Text())
		if err != nil {
			log.Fatal(err.Error())
		}
		nums_count += 1
		go getFactorsCount(num, counts)
	}

	factorsCountSum := 0
	for i := 0; i < nums_count; i++ {
		factorsCountSum += <-counts
	}
	workTime := time.Since(startTime).Seconds()

	fmt.Printf("Time: %f seconds\n", workTime)
	fmt.Printf("Result: %d\n", factorsCountSum)
}
