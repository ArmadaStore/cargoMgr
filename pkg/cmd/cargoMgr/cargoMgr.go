// cargo manager entry code
package cargoMgr

import (
	"github.com/ArmadaStore/cargo/pkg/lib"
)

func Run(port string) error {
	cargoMgrInfo := lib.Init(port)
	cargoMgrInfo.ListenRoutine()
}
