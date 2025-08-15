package raft

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type DataSaver struct {
	parent             *Raft
	logsFilename       string
	metadataFilename   string
	previousSavedIndex int

	mu sync.Mutex

	DataSaverFunctions
}

type DataSaverFunctions interface {
	SaveValues(currentTerm, votedFor, commitedLength int32, logs []LogEntry) (bool, error)
	LoadValues() (int, int, int, []LogEntry, error)

	saveLog(logs []LogEntry, file *os.File) (bool, error)
	loadLog() ([]LogEntry, error)
}

func NewRaftDataSaver(r *Raft, cfg *Config) *DataSaver {
	return &DataSaver{
		parent:             r,
		logsFilename:       cfg.valuesFilename,
		metadataFilename:   cfg.valuesFilename + ".meta",
		previousSavedIndex: 0,
	}
}

func ensureDir(filename string) error {
	dir := filepath.Clean(filepath.Dir(filename))

	info, err := os.Stat(dir)
	if err == nil {
		if info.IsDir() {
			return nil
		} else {
			return fmt.Errorf("path %q exists but is not a directory", dir)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to stat directory %q: %w", dir, err)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {

		return fmt.Errorf("failed to create directory %q: %w", dir, err)
	}
	return nil
}

// ------
// saving
// ------

func (rds *DataSaver) saveLog(logs []LogEntry, f *os.File) (int, error) {
	fmt.Printf("Saving logs, length: %d\n", len(logs))
	encoder := gob.NewEncoder(f)
	savedLines := 0
	for _, entry := range logs {
		if err := encoder.Encode(entry); err != nil {
			return savedLines, fmt.Errorf("encode log entry: %w", err)
		}
		savedLines += 1
	}
	if err := f.Sync(); err != nil {
		return savedLines, fmt.Errorf("sync log file: %w", err)
	}
	return savedLines, nil
}

func (rds *DataSaver) saveMetadata(currentTerm, votedFor, committedLength int, f *os.File) error {
	meta := struct {
		Term           int
		VotedFor       int
		CommitedLength int
	}{
		Term:           currentTerm,
		VotedFor:       votedFor,
		CommitedLength: committedLength,
	}
	encoder := gob.NewEncoder(f)
	if err := encoder.Encode(meta); err != nil {
		return fmt.Errorf("encode metadata: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync metadata: %w", err)
	}

	return nil
}

func (rds *DataSaver) handleLogFileExistanceAndCreation(path string) (*os.File, error) {
	dir := filepath.Dir(path)
	if err := ensureDir(dir); err != nil {
		return nil, fmt.Errorf("ensure values dir %q: %w", dir, err)
	}

	f, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0o644,
	)
	if err != nil {
		return nil, fmt.Errorf("open or create %q: %w", path, err)
	}

	return f, nil
}

func (rds *DataSaver) saveValuesManager(
	currentTerm, votedFor, committedLength int,
	logs []LogEntry,
) (bool, error) {
	logFile, err := rds.handleLogFileExistanceAndCreation(rds.logsFilename)
	if err != nil {
		fmt.Println("Failed to create logs file, error: ", err)
		return false, err
	}
	defer logFile.Close()

	metadataFile, err := os.Create(rds.metadataFilename)
	if err != nil {
		fmt.Println("Failed to create metadata file, error: ", err)
		return false, err
	}
	defer metadataFile.Close()

	err = rds.saveMetadata(currentTerm, votedFor, committedLength, metadataFile)
	if err != nil {
		fmt.Println("Failed to save metadata, error: ", err)
		return false, err
	}
	_, err = rds.saveLog(logs, logFile)
	if err != nil {
		fmt.Println("Failed to save logs, error: ", err)
		return false, err
	}
	// rds.previousSavedIndex += savedLines

	return true, nil
}

func (rds *DataSaver) SaveValues() (bool, error) {

	rds.mu.Lock()
	defer rds.mu.Unlock()
	rds.parent.mu.RLock()
	if rds.previousSavedIndex > len(rds.parent.log) {
		rds.previousSavedIndex = len(rds.parent.log)
	}
	idx := rds.previousSavedIndex

	currentTerm := rds.parent.currentTerm
	votedFor := rds.parent.votedFor
	committedLength := rds.parent.commitedLength

	fmt.Println("SaveValues idx: ", idx)
	src := rds.parent.log[idx:]
	logCopy := make([]LogEntry, len(src))
	copy(logCopy, src)
	rds.parent.mu.RUnlock()

	// if len(logCopy) == 0 && currentTerm == 0{
	// 	return false, nil
	// }

	ok, err := rds.saveValuesManager(
		currentTerm,
		votedFor,
		committedLength,
		logCopy,
	)
	if err != nil || !ok {
		return ok, err
	}
	rds.parent.mu.RLock()
	lengthNow := len(rds.parent.log)
	rds.parent.mu.RUnlock()

	newIndex := idx + len(logCopy)
	if newIndex > lengthNow {
		newIndex = lengthNow
	}
	rds.previousSavedIndex = newIndex
	return true, nil
}

// ------
// loading
// ------

func (rds *DataSaver) openFileForReading(
	path string,
) (*os.File, error) {
	dir := filepath.Dir(path)
	if err := ensureDir(dir); err != nil {
		return nil, fmt.Errorf("ensure dir %q: %w", dir, err)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file %q: %w", path, err)
	}
	return f, nil
}

func (rds *DataSaver) loadLogs(f *os.File) ([]LogEntry, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to start: %w", err)
	}

	dec := gob.NewDecoder(f)
	var logs []LogEntry
	for {
		var entry LogEntry
		if err := dec.Decode(&entry); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode log entry: %w", err)
		}
		logs = append(logs, entry)
	}
	return logs, nil
}

func (rds *DataSaver) loadMetadata(
	f *os.File,
) (currentTerm, votedFor, committedLength int, err error) {
	if _, err = f.Seek(0, 0); err != nil {
		err = fmt.Errorf("seek to start: %w", err)
		return
	}

	var meta struct {
		Term           int
		VotedFor       int
		CommitedLength int
	}
	dec := gob.NewDecoder(f)
	if err = dec.Decode(&meta); err != nil {
		err = fmt.Errorf("decode metadata: %w", err)
		return
	}

	currentTerm = meta.Term
	votedFor = meta.VotedFor
	committedLength = meta.CommitedLength
	return
}

func (rds *DataSaver) LoadValues() (int, int, int, []LogEntry, error) {
	rds.parent.mu.Lock()
	defer rds.parent.mu.Unlock()

	logFile, err := rds.openFileForReading(rds.logsFilename)
	if err != nil {
		fmt.Println("Error opening log file, error: ", err)
		return 0, 0, 0, nil, err
	}
	defer logFile.Close()

	metadataFile, err := rds.openFileForReading(rds.metadataFilename)
	if err != nil {
		return 0, 0, 0, nil, err
	}
	defer metadataFile.Close()

	currentTerm, votedFor, committedLen, err := rds.loadMetadata(metadataFile)
	if err != nil {
		return 0, 0, 0, nil, err
	}

	logs, err := rds.loadLogs(logFile)
	if err != nil {
		fmt.Println("Error reading log file, error: ", err)
		return 0, 0, 0, nil, err
	}
	rds.previousSavedIndex = len(logs)
	return currentTerm, votedFor, committedLen, logs, nil
}
