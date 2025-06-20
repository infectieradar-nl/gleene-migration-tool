package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"os"
)

// CSVRow represents a single row from the CSV file
type CSVRow struct {
	ParticipantID string
	PhoneNumber   string
	FirstName     string
	LastName      string
	Address       string
	City          string
	ZipCode       string
}

// CSVProcessor defines the interface for processing CSV rows
type CSVProcessor interface {
	ProcessRow(row CSVRow, rowIndex int) bool
}

// StreamCSV reads and processes CSV file line by line without loading entire file into memory
func StreamCSV(
	filePath string,
	separator string,
	processor CSVProcessor) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = rune(separator[0])

	// Read header row first
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV headers: %w", err)
	}

	if len(headers) < 4 {
		return fmt.Errorf("Could not find required headers in CSV file. Content of first row: %v", headers)
	}

	rowIndex := 0
	failures := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read CSV row %d: %w", rowIndex+1, err)
		}

		// Convert record to CSVRow (map[string]interface{})
		var row CSVRow
		for i, value := range record {
			if i < len(headers) {
				switch headers[i] {
				case "dn_extra_usn2":
					row.ParticipantID = value
				case "dn_telefoonnummer_1":
					row.PhoneNumber = value
				case "dn_naam":
					row.FirstName = value
				case "dn_achternaam":
					row.LastName = value
				case "dn_adres":
					row.Address = value
				case "dn_plaats":
					row.City = value
				case "dn_postcode":
					row.ZipCode = value
				}
			}
		}

		// Process the row
		if success := processor.ProcessRow(row, rowIndex); !success {
			failures++
		}

		rowIndex++

		// Log progress every 1000 rows
		if rowIndex%1000 == 0 {
			slog.Info("Processing progress", slog.Int("rows_processed", rowIndex))
		}
	}

	slog.Info("CSV processing completed", slog.Int("total_rows", rowIndex), slog.Int("failures", failures))
	return nil
}

// ReadCSVToSlice reads entire CSV into a slice of CSVRow (use only for small files)
func ReadCSVToSlice(filePath string, separator string) ([]CSVRow, error) {
	var rows []CSVRow

	processor := &sliceProcessor{rows: &rows}
	err := StreamCSV(filePath, separator, processor)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// sliceProcessor implements CSVProcessor to collect rows into a slice
type sliceProcessor struct {
	rows *[]CSVRow
}

func (p *sliceProcessor) ProcessRow(row CSVRow, rowIndex int) bool {
	*p.rows = append(*p.rows, row)
	return true
}
