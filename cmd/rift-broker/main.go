// Command rift-broker is the Rift broker entry point.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/jayyy255/rift/internal/version"
)

func main() {
	flags := flag.NewFlagSet("rift-broker", flag.ExitOnError)
	showVersion := flags.Bool("version", false, "print build version")
	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), "rift-broker - Rift broker (scaffold)")
		fmt.Fprintln(flags.Output(), "Usage: rift-broker [--help] [--version]")
		flags.PrintDefaults()
		fmt.Fprintln(flags.Output(), "Messaging services are not implemented yet.")
	}
	flags.Parse(os.Args[1:])
	if flags.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "rift-broker: unsupported command %q\n", flags.Arg(0))
		flags.Usage()
		os.Exit(2)
	}
	if *showVersion {
		fmt.Printf("rift-broker %s\n", version.Version)
		return
	}
	flags.SetOutput(os.Stdout)
	flags.Usage()
}
