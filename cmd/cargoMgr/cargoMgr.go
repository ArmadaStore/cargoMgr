// cargo manager entry point
package main

import (
	"os"

	"github.com/ArmadaStore/cargoMgr/pkg/cmd"
	"github.com/ArmadaStore/cargoMgr/pkg/cmd/cargoMgr"
)

func main() {
	// cargo manager listen port
	port := os.Args[1]

	err := cargoMgr.Run(port)
	cmd.CheckError(err)
}
