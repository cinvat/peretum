package main

import "github.com/cinvat/peretum/cmd"

func main() {
	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}
