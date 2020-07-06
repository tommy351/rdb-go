package main

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/tommy351/rdb-go"
)

// nolint: gochecknoglobals
var (
	outputFormat string

	rootCmd = &cobra.Command{
		Use:  "rdb [path]",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var printer Printer

			writer := bufio.NewWriter(os.Stdout)
			defer writer.Flush()

			switch outputFormat {
			case "json":
				printer = NewJSONPrinter(writer)
			default:
				// nolint: goerr113
				return fmt.Errorf("unsupported format %q", outputFormat)
			}

			return printParserData(args[0], printer)
		},
	}
)

func main() {
	rootCmd.PersistentFlags().StringVarP(&outputFormat, "output", "o", "json", "output format")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func printParserData(path string, printer Printer) error {
	file, err := os.Open(path)

	if err != nil {
		return err
	}

	defer file.Close()

	parser := rdb.NewParser(file)

	if err := printer.Start(); err != nil {
		return err
	}

	for {
		data, err := parser.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		switch v := data.(type) {
		case *rdb.StringData:
			err = printer.String(v)
		case *rdb.ListHead:
			err = printer.ListHead(v)
		case *rdb.ListEntry:
			err = printer.ListEntry(v)
		case *rdb.ListData:
			err = printer.ListData(v)
		case *rdb.SetHead:
			err = printer.SetHead(v)
		case *rdb.SetEntry:
			err = printer.SetEntry(v)
		case *rdb.SetData:
			err = printer.SetData(v)
		case *rdb.SortedSetHead:
			err = printer.SortedSetHead(v)
		case *rdb.SortedSetEntry:
			err = printer.SortedSetEntry(v)
		case *rdb.SortedSetData:
			err = printer.SortedSetData(v)
		case *rdb.HashHead:
			err = printer.HashHead(v)
		case *rdb.HashEntry:
			err = printer.HashEntry(v)
		case *rdb.HashData:
			err = printer.HashData(v)
		}

		if err != nil {
			return err
		}
	}

	return printer.End()
}
