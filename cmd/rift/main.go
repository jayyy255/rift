// Command rift is the Rift administration CLI entry point.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/jayyy255/rift/internal/version"
)

func main() {
	flags := flag.NewFlagSet("rift", flag.ExitOnError)
	showVersion := flags.Bool("version", false, "print build version")
	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), "rift - Rift administration CLI (scaffold)")
		fmt.Fprintln(flags.Output(), "Usage: rift [--help] [--version]")
		flags.PrintDefaults()
		fmt.Fprintln(flags.Output(), "Messaging services are not implemented yet.")
	}
	flags.Parse(os.Args[1:])
	if flags.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "rift: unsupported command %q\n", flags.Arg(0))
		flags.Usage()
		os.Exit(2)
	}
	if *showVersion {
		fmt.Printf("rift %s\n", version.Version)
		return
	}
	flags.SetOutput(os.Stdout)
	flags.Usage()
}
