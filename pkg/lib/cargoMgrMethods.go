// cargo manager methods

package lib

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/google/uuid"
	"github.com/mmcloughlin/geohash"

	"github.com/ArmadaStore/cargoMgr/pkg/cmd"
	"github.com/ArmadaStore/comms/rpc/cargoToMgr"
)

type CargoComm struct {
	cargoToMgr.UnimplementedRpcCargoToMgrServer

	cargoMgrInfo *CargoMgrInfo
}

type CargoNode struct {
	ID    string // <geohash + uuid>
	Lat   float64
	Lon   float64
	TSize float64
	RSize float64
}

type CargoMgrInfo struct {
	Port   string
	CC     CargoComm
	Cargos map[string]CargoNode
}

func Init(port string) *CargoMgrInfo {
	var cargoMgrInfo CargoMgrInfo
	cargoMgrInfo.Port = port
	cargoMgrInfo.CC.cargoMgrInfo = &cargoMgrInfo

	//fmt.Fprintf(os.Stderr, "Port number %s", cargoMgrInfo.Port)

	return &cargoMgrInfo
}

func (cc *CargoComm) RegisterToMgr(ctx context.Context, cargoInfo *cargoToMgr.CargoInfo) (*cargoToMgr.Ack, error) {
	newCargoNode := CargoNode{
		ID:    "",
		Lat:   cargoInfo.GetLat(),
		Lon:   cargoInfo.GetLon(),
		TSize: cargoInfo.GetTSize(),
		RSize: cargoInfo.GetTSize(),
	}
	geohashIDstr := geohash.Encode(newCargoNode.Lat, newCargoNode.Lon)
	uuID, err := uuid.NewUUID()
	cmd.CheckError(err)
	cargoID := geohashIDstr + "-" + uuID.String()
	newCargoNode.ID = cargoID

	cc.cargoMgrInfo.Cargos[cargoID] = newCargoNode

	fmt.Fprintf(os.Stderr, "%v\n", cc.cargoMgrInfo.Cargos)

	return &cargoToMgr.Ack{ID: cargoID, Ack: "Registered cargo node"}, nil
}

func (cargoMgrInfo *CargoMgrInfo) ListenRoutine(wg *sync.WaitGroup) {
	defer wg.Done()

	listen, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", cargoMgrInfo.Port))
	cmd.CheckError(err)

	server := grpc.NewServer()
	cargoToMgr.RegisterRpcCargoToMgrServer(server, &(cargoMgrInfo.CC))

	reflection.Register(server)

	err = server.Serve(listen)
	cmd.CheckError(err)
}
