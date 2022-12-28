package common

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"sync"
)

func ReadCsv(p string) (ch chan map[string]string, err error) {
	info, err := os.Stat(p)
	if err != nil {
		return nil, err
	}

	if info.IsDir() {
		return readCsvPath(p)
	}
	return readCsvFile(p)
}

func readCsvPath(p string) (ch chan map[string]string, err error) {
	dirs, err := os.ReadDir(p)
	if err != nil {
		return nil, err
	}
	var wait sync.WaitGroup
	ch = make(chan map[string]string, 100)

	for _, dir := range dirs {
		if dir.IsDir() {
			continue
		}

		if !strings.HasSuffix(dir.Name(), ".csv") {
			continue
		}

		wait.Add(1)
		f, err := os.Open(path.Join(p, dir.Name()))
		if err != nil {
			return nil, err
		}

		go func(file *os.File) {
			defer wait.Done()
			defer func() {
				_ = file.Close()
			}()
			reader := csv.NewReader(f)
			header, err := reader.Read()
			if err != nil {
				log.Println("## read csv file error ", err)
				return
			}
			for {
				records, err := reader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Println("## read csv file error ", err)
					return
				}
				data := make(map[string]string, len(header))
				for i, h := range header {
					data[h] = records[i]
				}
				ch <- data
			}
		}(f)
	}

	go func() {
		wait.Wait()
		close(ch)
	}()

	return
}

func readCsvFile(p string) (ch chan map[string]string, err error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	ch = make(chan map[string]string, 100)
	go func() {
		defer close(ch)
		defer func() {
			_ = f.Close()
		}()
		reader := csv.NewReader(f)
		header, err := reader.Read()
		if err != nil {
			log.Println("## read csv file error ", err)
			return
		}
		for {
			records, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Println("## read csv file error ", err)
				return
			}
			data := make(map[string]string, len(header))
			for i, h := range header {
				data[h] = records[i]
			}
			ch <- data
		}
	}()

	return
}
