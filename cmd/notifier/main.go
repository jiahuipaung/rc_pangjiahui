package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jiahuipaung/rc_pangjiahui/internal/app"
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
	runtimeConfig, err := config.LoadRuntime(role, os.LookupEnv)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := app.RunConfigured(ctx, runtimeConfig); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
