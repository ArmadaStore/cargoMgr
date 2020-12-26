// cargo manager entry code
package cargoMgr

import (
	"sync"

	"github.com/ArmadaStore/cargoMgr/pkg/lib"
)

func Run(port string) error {
	cargoMgrInfo := lib.Init(port)

	var wg sync.WaitGroup
	wg.Add(1)
	cargoMgrInfo.ListenRoutine(&wg)
	wg.Wait()
}
