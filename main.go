package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func main() {
	st := time.Now()
	input_path := "/home/ajeeb/sandbox/src/GMT+PrevClose/SpotData22"
	paths := []string{}
	//pathcount := 0
	filepath.Walk(input_path, func(path string, info os.FileInfo, err error) error {
		if err == nil && strings.Contains(info.Name(), ".csv") {
			paths = append(paths, path)
			//pathcount++
		}
		return nil
	})
	//log.Printf("Paths added %d", pathcount)
	//var wg sync.WaitGroup
	for _, filepath := range paths {
		//	if count%19 == 0 {
		//		wg.Wait()
		//	}
		//	wg.Add(1)
		path_name := strings.Split(filepath, "/")
		name := strings.Trim(path_name[len(path_name)-1], ".csv")
		output_path := fmt.Sprintf("%s_out.csv", name)
		//go func(filepath string, output_path string) {
		//	defer wg.Done()
		fmt.Printf("\n%v", filepath)
		Close_Values(filepath, output_path)
		//}(filepath, output_path)
	}
	//wg.Wait()
	fmt.Println("Done. \n Time Taken: ", time.Since(st))
}

func Close_Values(inPath string, outPath string) {
	input_file := inPath
	output_file := outPath
	file1, _ := os.Open(input_file)
	csvReader := csv.NewReader(file1)
	file2, _ := os.Create(output_file)
	csvWriter := csv.NewWriter(file2)
	header := []string{"datetime", "open", "high", "low", "close", "volume", "prevclose"}
	csvWriter.Write(header)
	contents, _ := csvReader.ReadAll()
	var prev_close float64
	for i := 1; i <= len(contents)-1; i++ {
		ts1, _ := strconv.ParseInt(contents[i][0], 10, 64)
		o, _ := strconv.ParseFloat(contents[i][1], 64)
		h, _ := strconv.ParseFloat(contents[i][2], 64)
		l, _ := strconv.ParseFloat(contents[i][3], 64)
		c, _ := strconv.ParseFloat(contents[i][4], 64)
		v, _ := strconv.ParseFloat(contents[i][5], 64)

		spot := []string{
			fmt.Sprintf("%d", ts1+19800),
			fmt.Sprintf("%0.2f", o),
			fmt.Sprintf("%0.2f", h),
			fmt.Sprintf("%0.2f", l),
			fmt.Sprintf("%0.2f", c),
			fmt.Sprintf("%0.2f", v),
			fmt.Sprintf("%0.2f", prev_close),
		}
		if i != len(contents)-1 {
			ts2, _ := strconv.ParseInt(contents[i+1][0], 10, 64)
			if ts2-ts1 > 60 {
				prev_close = c
			}
		}
		csvWriter.Write(spot)
		csvWriter.Flush()
	}
}
