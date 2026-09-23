package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/jiahuipaung/rc_pangjiahui/internal/config"
)

func main() {
	roleValue := flag.String("role", "all", "process role: api, publisher, worker, scheduler, or all")
	flag.Parse()
	role, err := config.ParseRole(*roleValue)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	fmt.Fprintf(os.Stdout, "notifier role %s configuration valid\n", role)
}
