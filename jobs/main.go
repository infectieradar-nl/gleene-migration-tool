package main

import (
	"log/slog"
	"time"
)

func main() {
	slog.Info("Start migration job")
	start := time.Now()

	processor := &MigrateDataProcessor{}

	err := StreamCSV(conf.InfoCsv, conf.CSVSeparator, processor)
	if err != nil {
		slog.Error("Failed to process CSV", slog.String("error", err.Error()))
		return
	}

	slog.Info("CSV processing completed",
		slog.Int("total_processed", processor.processedCount),
		slog.String("duration", time.Since(start).String()))
}
