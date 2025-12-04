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

	slog.Info("Starting Controller Service...")

	// TODO: Initialize Raft for Control Plane
	// TODO: Initialize Cluster Metadata State Machine

	select {} // Block forever for now
}
