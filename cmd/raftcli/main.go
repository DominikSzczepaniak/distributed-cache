package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/dominikszczepaniak/distributed-cache/pkg/client"
)

type CLI struct {
	client *client.SmartClient
}

func NewCLI(controllers []string) (*CLI, error) {
	sc, err := client.NewSmartClient(controllers)
	if err != nil {
		return nil, err
	}
	return &CLI{
		client: sc,
	}, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: raftcli <controller-addresses>")
		fmt.Println("Example: raftcli localhost:8080,localhost:8081")
		os.Exit(1)
	}

	// Parse comma-separated controller addresses
	controllerAddrs := strings.Split(os.Args[1], ",")
	for i, addr := range controllerAddrs {
		addr = strings.TrimSpace(addr)
		if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
			addr = "http://" + addr
		}
		controllerAddrs[i] = addr
	}

	cli, err := NewCLI(controllerAddrs)
	if err != nil {
		fmt.Printf("Failed to initialize client: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("==========================================")
	fmt.Println("  Raft Distributed Cache - Smart CLI")
	fmt.Println("==========================================")
	fmt.Printf("Connected to controllers: %v\n", controllerAddrs)
	fmt.Println("Type 'help' for available commands")
	fmt.Println()

	cli.Run()
}

func (c *CLI) Run() {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")

		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if err := c.executeCommand(line); err != nil {
			fmt.Printf("Error: %v\n", err)
		}
	}
}

func (c *CLI) executeCommand(line string) error {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return nil
	}

	cmd := strings.ToLower(parts[0])
	args := parts[1:]

	switch cmd {
	case "put":
		return c.cmdPut(args)
	case "get":
		return c.cmdGet(args)
	case "help":
		c.cmdHelp()
		return nil
	case "exit", "quit":
		fmt.Println("Goodbye!")
		os.Exit(0)
	default:
		return fmt.Errorf("unknown command: %s (type 'help' for available commands)", cmd)
	}
	return nil
}

func (c *CLI) cmdPut(args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: put <key> <value>")
	}

	key := args[0]
	value := args[1]

	err := c.client.Put(key, value)
	if err != nil {
		return fmt.Errorf("PUT failed: %v", err)
	}

	fmt.Printf("✓ PUT successful: key=%s, value=%s\n", key, value)
	return nil
}

func (c *CLI) cmdGet(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: get <key>")
	}

	key := args[0]

	val, err := c.client.Get(key)
	if err != nil {
		return fmt.Errorf("GET failed: %v", err)
	}

	fmt.Printf("✓ GET successful: key=%s, value=%s\n", key, val)
	return nil
}

func (c *CLI) cmdHelp() {
	fmt.Println("Available Commands:")
	fmt.Println("----------------------------------------")
	fmt.Println("  put <key> <value>  - Store a key-value pair")
	fmt.Println("  get <key>          - Retrieve value for key")
	fmt.Println("  help               - Show this help message")
	fmt.Println("  exit               - Exit the CLI")
	fmt.Println("----------------------------------------")
}
