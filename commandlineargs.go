package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

// Define a custom type that implements flag.Value
type Choice string

// Allowed choices
var allowed = []string{"low", "medium", "high"}

// Implement flag.Value interface
func (c *Choice) String() string {
	return string(*c)
}

func (c *Choice) Set(value string) error {
	for _, v := range allowed {
		if value == v {
			*c = Choice(value)
			return nil
		}
	}
	return fmt.Errorf("invalid value %q, must be one of %v", value, allowed)
}

func main() {
	var level Choice

	// Register custom flag
	flag.Var(&level, "level", fmt.Sprintf("set level (%s)", strings.Join(allowed, "|")))

	flag.Parse()

	if level == "" {
		fmt.Fprintln(os.Stderr, "Error: --level is required")
		flag.Usage()
		os.Exit(1)
	}

	fmt.Println("Selected level:", level)
}
