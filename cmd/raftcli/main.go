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
	case "delete", "del":
		return c.cmdDelete(args)
	case "load":
		return c.cmdLoad(args)
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

func (c *CLI) cmdDelete(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: delete <key>")
	}

	key := args[0]

	err := c.client.Delete(key)
	if err != nil {
		return fmt.Errorf("DELETE failed: %v", err)
	}

	fmt.Printf("✓ DELETE successful: key=%s\n", key)
	return nil
}

func (c *CLI) cmdLoad(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: load <count>")
	}

	var count int
	if _, err := fmt.Sscanf(args[0], "%d", &count); err != nil {
		return fmt.Errorf("invalid count: %v", err)
	}

	fmt.Printf("Loading %d keys...\n", count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("val-%d", i)
		if err := c.client.Put(key, value); err != nil {
			fmt.Printf("Failed to put %s: %v\n", key, err)
		}
		if i%100 == 0 {
			fmt.Printf(".")
		}
	}
	fmt.Println("\nDone.")
	return nil
}

func (c *CLI) cmdHelp() {
	fmt.Println("Available Commands:")
	fmt.Println("----------------------------------------")
	fmt.Println("  put <key> <value>  - Store a key-value pair")
	fmt.Println("  get <key>          - Retrieve value for key")
	fmt.Println("  delete <key>       - Delete a key")
	fmt.Println("  load <count>       - Load N keys (key-i, val-i)")
	fmt.Println("  help               - Show this help message")
	fmt.Println("  exit               - Exit the CLI")
	fmt.Println("----------------------------------------")
}
