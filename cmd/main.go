package main

import (
	"context"
	"flag"
	"log"
	"os"
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
