// Functions for reading and writing Raft's persistent state (current term, voted for, log entries) to disk.
// Logic for recovering state on restart.
package raft

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type RaftDataSaver struct {
	parent         *Raft
	valuesFilename string

	RaftDataSaverFunctions
}

type RaftDataSaverFunctions interface {
	SaveValues(currentTerm, votedFor, commitedLength int32, logs []LogEntry) (bool, error)
	LoadValues() (int, int, int, []LogEntry, error)

	saveLog(logs []LogEntry, file *os.File) (bool, error)
	loadLog() ([]LogEntry, error)
}

func NewRaftDataSaver(r *Raft, cfg *Config) *RaftDataSaver {
	return &RaftDataSaver{
		parent:         r,
		valuesFilename: cfg.valuesFilename,
	}
}

func (rds *RaftDataSaver) saveLog(logs []LogEntry, file *os.File) (bool, error) {
	for _, entry := range logs {
		switch entry.message.msgType {
		case put:
			if entry.message.value == nil {
				return false, fmt.Errorf(
					"nil value for PUT entry: term=%d key=%d",
					entry.term, entry.message.key,
				)
			}
			if _, err := fmt.Fprintf(
				file,
				"%d %s %d %d\n",
				entry.term,
				entry.message.msgType,
				entry.message.key,
				*entry.message.value,
			); err != nil {
				return false, fmt.Errorf("failed to write PUT entry: %w", err)
			}

		case get, delete:
			if _, err := fmt.Fprintf(
				file,
				"%d %s %d\n",
				entry.term,
				entry.message.msgType,
				entry.message.key,
			); err != nil {
				return false, fmt.Errorf(
					"failed to write %s entry: %w",
					entry.message.msgType,
					err,
				)
			}

		default:
			return false, fmt.Errorf(
				"unknown message type %q in log entry", entry.message.msgType,
			)
		}
	}
	return true, nil
}

func (rds *RaftDataSaver) SaveValues(currentTerm, votedFor, commitedLength int32, logs []LogEntry) (bool, error) {
	file, err := os.Create(rds.valuesFilename)
	if err != nil {
		panic("Path of VALUES_FILENAME is not correct, cannot save data to disk")
	}
	defer file.Close()
	if _, err := fmt.Fprintf(
		file,
		"%d %d %d\n",
		currentTerm,
		votedFor,
		commitedLength,
	); err != nil {
		return false, fmt.Errorf("failed to write header: %w", err)
	}

	return rds.saveLog(logs, file)
}

func (rds *RaftDataSaver) loadLog() ([]LogEntry, error) {
	file, err := os.Open(rds.valuesFilename)
	if err != nil {
		return nil, fmt.Errorf(
			"unable to open values file %q: %w",
			rds.valuesFilename, err,
		)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("read header for logs: %w", err)
		}
		return nil, nil
	}

	var logs []LogEntry
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 3 {
			return nil, fmt.Errorf("invalid log line: %q", line)
		}
		t, err := strconv.ParseInt(fields[0], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid term %q: %w", fields[0], err)
		}
		msgType := MessageType(fields[1])
		k, err := strconv.ParseInt(fields[2], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid key %q: %w", fields[2], err)
		}

		var valuePtr *int
		if msgType == put {
			if len(fields) != 4 {
				return nil, fmt.Errorf("PUT entry malformed: %q", line)
			}
			v, err := strconv.ParseInt(fields[3], 10, 32)
			if err != nil {
				return nil, fmt.Errorf(
					"invalid value %q: %w", fields[3], err,
				)
			}
			vi := int(v)
			valuePtr = &vi
		} else {
			if len(fields) != 3 {
				return nil, fmt.Errorf(
					"%s entry malformed: %q", msgType, line,
				)
			}
		}

		logs = append(logs, LogEntry{
			term: int(t),
			message: Message{
				msgType: msgType,
				key:     int(k),
				value:   valuePtr,
			},
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning logs: %w", err)
	}
	return logs, nil
}

func (rds *RaftDataSaver) LoadValues() (int, int, int, []LogEntry, error) {
	file, err := os.Open(rds.valuesFilename)
	if err != nil {
		return 0, 0, 0, nil, fmt.Errorf(
			"unable to open values file %q: %w",
			rds.valuesFilename, err,
		)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return 0, 0, 0, nil, fmt.Errorf("read header: %w", err)
		}
		return 0, 0, 0, nil, fmt.Errorf("empty values file")
	}
	header := scanner.Text()
	var currentTerm, votedFor, committedLen int
	n, err := fmt.Sscanf(header, "%d %d %d",
		&currentTerm, &votedFor, &committedLen,
	)
	if err != nil || n != 3 {
		return 0, 0, 0, nil, fmt.Errorf(
			"invalid header format %q", header,
		)
	}

	logs, err := rds.loadLog()
	if err != nil {
		return 0, 0, 0, nil, err
	}
	return currentTerm, votedFor, committedLen, logs, nil
}
