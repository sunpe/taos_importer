package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"taos_importer/internal/common"
	"taos_importer/internal/config"
	"taos_importer/internal/db_table"
	"taos_importer/internal/field"
	"taos_importer/internal/importer"

	"github.com/pelletier/go-toml/v2"
)

func main() {
	ctx := context.Background()
	importCmd := flag.NewFlagSet("import", flag.ExitOnError)
	confFile := importCmd.String("conf", "", "config file path. Required!")
	autoCreate := importCmd.Bool("auto-create", true, "auto create database, stable, tables. Optional, default is true")
	outputFile := importCmd.String("output-file", "", "output file path. Optional, default is local path")

	if len(os.Args) < 2 {
		log.Printf("## param error %v", os.Args[1:])
		os.Exit(1)
	}

	if confFile == nil {
		log.Println("## param error, conf is null")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "import":
		_ = importCmd.Parse(os.Args[2:])
		importData(ctx, *confFile, autoCreate, outputFile)
	default:
		log.Printf("## unknown command %s ", os.Args[1])
		os.Exit(1)
	}
}

func importData(ctx context.Context, configFile string, autoCreate *bool, outputFile *string) {
	log.Println("## start to import data. config file is ", configFile)
	f, err := os.Open(configFile)
	if err != nil {
		log.Printf("## open config file [%s] error %v", configFile, err)
		os.Exit(1)
	}
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(f)
	if err != nil {
		log.Printf("## read config file content [%s] error %v", configFile, err)
		os.Exit(1)
	}

	var conf config.Config
	if err = toml.Unmarshal(b, &conf); err != nil {
		log.Printf("## read config file [%s] fail %v", configFile, err)
		os.Exit(1)
	}

	if conf.Pprof {
		go func() {
			if err := http.ListenAndServe(":8000", nil); err != nil {
				panic(err)
			}
		}()
	}

	if autoCreate != nil && *autoCreate {
		conf.AutoCreate = *autoCreate
		conf.OutputFile = *outputFile
	}
	if len(conf.OutputFile) == 0 {
		conf.OutputFile = "./importer.log"
	}

	output, err := os.OpenFile(conf.OutputFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		log.Printf("## open log fail [%s] fail %v", conf.OutputFile, err)
		os.Exit(1)
	}
	defer func() { _ = output.Close() }()

	logfile := bufio.NewWriter(output)
	defer func() { _ = logfile.Flush() }()

	// todo create db, stable
	// create child table
	tableNames := createTables(ctx, conf, conf.AutoCreate)
	// import data
	ch := make(chan string, 100)
	go importDataToTable(ctx, conf, ch, tableNames)

	for msg := range ch {
		_, _ = logfile.WriteString(msg)
		_, _ = logfile.WriteString("\n")
	}

	log.Println("## importing data finished. config file is ", configFile)
}

// nolint
func createDB(ctx context.Context, conf config.Config) {
	dbUri := getDBUri(conf)
	dt, err := db_table.NewDatabaseAndTable(dbUri)
	if err != nil {
		log.Printf("## connect to database %s error %v ", dbUri, err)
		os.Exit(1)
	}
	var param db_table.DBParam
	param.DBName = conf.DB.Name
	if conf.DB.Buffer > 0 {
		param.Buffer = conf.DB.Buffer
	}
	if len(conf.DB.CacheModel) > 0 {
		param.CacheModel = conf.DB.CacheModel
	}
	if conf.DB.CacheSize > 0 {
		param.CacheSize = conf.DB.CacheSize
	}
	if len(conf.DB.Duration) > 0 {
		param.Duration = conf.DB.Duration
	}
	if conf.DB.Keep > 0 {
		param.Keep = conf.DB.Keep
	}
	if len(conf.DB.Precision) > 0 {
		param.Precision = conf.DB.Precision
	}
	if conf.DB.VGroups > 0 {
		param.VGroups = conf.DB.VGroups
	}
	if err = dt.CreateDB(ctx, param); err != nil {
		log.Printf("## create database %s error %v", conf.DB.Name, err)
		os.Exit(1)
	}
}

// nolint
func createSTable(ctx context.Context, conf config.Config) {
	dbUri := getDBUri(conf)
	dt, err := db_table.NewDatabaseAndTable(dbUri)
	if err != nil {
		log.Printf("## connect to database %s error %v", dbUri, err)
		os.Exit(1)
	}
	sql := "" // todo
	if err = dt.CreateSTableBySql(ctx, conf.DB.Name, sql); err != nil {
		log.Printf("## create stable by sql %s error %v", sql, err)
		os.Exit(1)
	}
}

func createTables(ctx context.Context, conf config.Config, autoCreate bool) (tables map[string]struct{}) {
	tableFiles, err := getFiles(conf.TagsDir, conf.TagsFiles, conf.TagsFileSuffix, "", nil)
	if err != nil {
		log.Printf("## get tag file error %v", err)
		os.Exit(1)
	}

	dbUri := getDBUri(conf)
	dt, err := db_table.NewDatabaseAndTable(dbUri)
	if err != nil {
		log.Printf("## connect to database %s error %v", dbUri, err)
		os.Exit(1)
	}

	tbNameCh := make(chan string, 10)
	tables = make(map[string]struct{}, 100)
	var wait sync.WaitGroup

	for i := 0; i < 10; i++ {
		wait.Add(1)

		go func(files chan string, w *sync.WaitGroup) {
			defer w.Done()
			for file := range files {
				ch, err := common.ReadCsv(file)
				if err != nil {
					log.Println("## read tag fail error", err)
					os.Exit(1)
				}

				for line := range ch {
					param, tbCode := tableParam(conf.DB.Name, conf.STable.Name, conf.STable.ChildTableName, line, conf.STable.Tags)
					tbNameCh <- tbCode

					if !autoCreate {
						continue
					}
					if err = dt.CreateTable(ctx, param); err != nil {
						log.Printf("## create table [%s] error %v \n", param.TableName, err)
						os.Exit(1)
					}
				}
			}
		}(tableFiles, &wait)
	}

	go func() {
		defer close(tbNameCh)
		wait.Wait()
	}()

	for tbName := range tbNameCh {
		tables[tbName] = struct{}{}
	}

	return
}

func tableParam(db, stable, tableNamePattern string, line map[string]string, tags []config.Column) (db_table.TableParam, string) {
	lineData := common.StrMap2AnyMap(line)
	tableName, err := db_table.GenerateTableName(tableNamePattern, lineData)
	if err != nil {
		log.Printf("## get table name -[%s] error %v", tableNamePattern, err)
		os.Exit(1)
	}
	tagValues := make([]db_table.TagValue, 0, len(tags))

	for _, tag := range tags {
		tagValue, err := field.DefaultExtractor.Extract(tag.Source, lineData)
		if err != nil {
			log.Printf("## get tag -[%s] value error %v", tag.Field, err)
			os.Exit(1)
		}
		tagValues = append(tagValues, db_table.TagValue{
			TagName:      tag.Field,
			TagValue:     tagValue,
			TagValueType: tag.Type,
		})
	}

	return db_table.TableParam{DBName: db, STableName: stable, TableName: tableName, TagValues: tagValues}, tableName
}

func importDataToTable(ctx context.Context, conf config.Config, ch chan string, tableNames map[string]struct{}) {
	defer close(ch)
	//
	dataFiles, err := getFiles(conf.DataDir, conf.DataFiles, conf.DataFileSuffix, conf.STable.ChildTableNamePrefix, tableNames)
	if err != nil {
		log.Println("## get data file fail.", err)
		os.Exit(1)
	}

	var wait sync.WaitGroup

	for i := 0; i < conf.DealOneTime; i++ {
		wait.Add(1)
		go doImport(ctx, conf, dataFiles, &wait, ch)
	}
	wait.Wait()
}

func doImport(ctx context.Context, conf config.Config, files chan string, w *sync.WaitGroup, messages chan string) {
	defer w.Done()

	for f := range files {
		ext := path.Ext(f)
		if ext == ".csv" {
			msg, err := importCsvData(ctx, conf, f)
			if err != nil {
				log.Printf("## import data file [%s] to tdengine fail. %v", f, err)
			}
			messages <- msg
		}
		// todo
	}
}

func importCsvData(ctx context.Context, conf config.Config, file string) (string, error) {
	table := getTableName(file, conf.STable.ChildTableNamePrefix)
	ci, err := importer.NewCsvImporter(conf, table)
	if err != nil {
		return "", err
	}
	err = ci.Import(ctx, file)
	msg := fmt.Sprintf("## importe file [%s] finished. total data-[%d] error count-[%d] start-[%s] end-[%v] spend-[%d] ms",
		file, ci.Total.Load(), ci.ErrorCount.Load(), ci.Start.Format("2006-01-02 15:04:05.000"),
		ci.End.Format("2006-01-02 15:04:05.000"), ci.End.Sub(ci.Start).Milliseconds())
	return msg, err
}

func getDBUri(conf config.Config) string {
	return fmt.Sprintf("%s:%s/tcp(%s:%d)/", conf.TDEngine.User, conf.TDEngine.Password, conf.TDEngine.Host, conf.TDEngine.Port)
}

func getTableName(file string, prefix string) string {
	// 用 file name 做 table name
	_, fileName := path.Split(file)
	ext := path.Ext(file)
	fileName = strings.TrimRight(fileName, ext)

	return prefix + fileName // todo
}

func getFiles(dataDir string, dataFiles []string, suffix string, tablePrefix string, tableNames map[string]struct{}) (chan string, error) {
	if len(dataDir) == 0 && len(dataFiles) == 0 {
		log.Println("## config error, dir config and files config is null")
		os.Exit(1)
	}

	if len(dataDir) == 0 && len(dataFiles) != 0 {
		return getFilesFromFilesConf(dataFiles, tablePrefix, tableNames), nil
	}

	if len(dataDir) != 0 && len(dataFiles) == 0 {
		return getFilesFromDir(dataDir, suffix, tablePrefix, tableNames)
	}

	return getFilesFromDirAndFiles(dataDir, dataFiles, tablePrefix, tableNames), nil
}

func getFilesFromFilesConf(dataFiles []string, tablePrefix string, tableNames map[string]struct{}) chan string {
	files := make(chan string, 10)

	go func() {
		defer close(files)
		for _, file := range dataFiles {
			if !filterByTableName(file, tablePrefix, tableNames) {
				continue
			}
			files <- file
		}
	}()

	return files
}

func getFilesFromDir(dataDir string, suffix string, tablePrefix string, tableNames map[string]struct{}) (chan string, error) {
	files := make(chan string, 10)

	go func() {
		defer close(files)
		fs, err := listDir(dataDir, suffix)
		if err != nil {
			log.Printf("## read file from dir-[%s] error %v", dataDir, err)
			os.Exit(1)
		}
		for _, f := range fs {
			if !filterByTableName(f, tablePrefix, tableNames) {
				continue
			}
			files <- f
		}
	}()

	return files, nil
}

func listDir(dir string, suffix string) (files []string, err error) {
	dirFiles, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range dirFiles {
		if file.IsDir() {
			fs, _ := listDir(path.Join(dir, file.Name()), suffix)
			files = append(files, fs...)
		}

		if !strings.HasSuffix(file.Name(), suffix) {
			continue
		}

		files = append(files, path.Join(dir, file.Name()))
	}
	return
}

func getFilesFromDirAndFiles(dataDir string, dataFiles []string, tablePrefix string, tableNames map[string]struct{}) chan string {
	files := make(chan string, 10)

	go func() {
		defer close(files)
		for _, f := range dataFiles {
			var abs string
			if filepath.IsAbs(f) {
				abs = f
			} else {
				abs = path.Join(dataDir, f)
			}

			if !filterByTableName(abs, tablePrefix, tableNames) {
				continue
			}

			files <- abs
		}
	}()

	return files
}

func filterByTableName(file string, prefix string, tables map[string]struct{}) bool {
	if len(tables) == 0 {
		return true
	}
	_, fileName := path.Split(file)
	ext := path.Ext(file)
	fileName = strings.Trim(fileName, ext)
	fileName = prefix + fileName
	_, exist := tables[fileName]
	return exist
}
