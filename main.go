package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"golang-insert-massive-data/app"
	"golang-insert-massive-data/helper"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	totalWorker = 100
)

var (
	dataHeaders = []string{
		"global_rank",
		"tld_rank",
		"domain",
		"tld",
		"ref_sub_nets",
		"ref_ips",
		"idn_domain",
		"idn_tld",
		"prev_global_rank",
		"prev_id_rank",
		"prev_ref_subnets",
		"prev_ref_ips",
	}
)

func openCSVFile() (*csv.Reader, *os.File) {
	log.Println("==> Open CSV File")
	csvFile, err := os.Open("./data/majestic_million.csv")
	helper.PanicIfError(err)
	csvReader := csv.NewReader(csvFile)
	return csvReader, csvFile
}

func readCsv(csvReader *csv.Reader, jobs chan<- []interface{}, wg *sync.WaitGroup) {
	isHeader := true
	for {
		row, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		if isHeader {
			isHeader = false
			continue
		}
		rowOrdered := make([]interface{}, 0)
		for _, each := range row {
			rowOrdered = append(rowOrdered, each)
		}
		wg.Add(1)
		jobs <- rowOrdered
	}
	close(jobs)
}

func dispatchWorkers(db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
	for workerIndex := 1; workerIndex <= totalWorker; workerIndex++ {
		go func(workerIndex int) {
			counterInc := 0
			for job := range jobs {
				insertData(workerIndex, counterInc, db, job)
				wg.Done()
				counterInc++
			}
		}(workerIndex)
	}
}

func generateQuestionsMark(n int) []string {
	s := make([]string, 0)
	for i := 0; i < n; i++ {
		s = append(s, "?")
	}
	return s
}

func insertData(workerIndex, counterInc int, db *sql.DB, valuesData []interface{}) {
	for {
		var outerError error
		func(outerError *error) {
			defer func() {
				if err := recover(); err != nil {
					*outerError = fmt.Errorf("%v", err)
				}
			}()

			conn, err := db.Conn(context.Background())
			query := fmt.Sprintf("INSERT INTO domain (%s) VALUES (%s)",
				strings.Join(dataHeaders, ","),
				strings.Join(generateQuestionsMark(len(dataHeaders)), ","),
			)

			_, err = conn.ExecContext(context.Background(), query, valuesData...)
			if err != nil {
				log.Fatal(err.Error())
			}

			err = conn.Close()
			if err != nil {
				log.Fatal(err.Error())
			}
		}(&outerError)
		if outerError == nil {
			break
		}
	}
	if counterInc%100 == 0 {
		log.Println("==> Worker", workerIndex, "Inserted", counterInc, "Data")
	}
}

func main() {
	start := time.Now()
	db := app.SetupNewDBConn()
	csvReader, csvFile := openCSVFile()
	defer func() {
		err := csvFile.Close()
		helper.PanicIfError(err)
	}()

	jobs := make(chan []interface{}, 0)
	wg := new(sync.WaitGroup)
	go dispatchWorkers(db, jobs, wg)
	readCsv(csvReader, jobs, wg)
	wg.Wait()
	duration := time.Since(start)
	fmt.Println("Done in", int(math.Ceil(duration.Seconds())), "seconds")
}
