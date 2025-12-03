package main

import (
	"log/slog"
	"os"
)

func main() {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))
	slog.SetDefault(logger)

	slog.Info("Starting DataNode Service...")

	// TODO: Initialize Sharded Cache
	// TODO: Initialize gRPC Data Plane
	
	select {} // Block forever for now
}