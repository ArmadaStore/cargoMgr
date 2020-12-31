// cargo manager methods

package lib

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/google/uuid"
	"github.com/mmcloughlin/geohash"

	"github.com/ArmadaStore/cargoMgr/pkg/cmd"
	"github.com/ArmadaStore/comms/rpc/cargoToMgr"
	"github.com/ArmadaStore/comms/rpc/taskToCargoMgr"
)

type TaskComm struct {
	taskToCargoMgr.UnimplementedRpcTaskToCargoMgrServer

	cargoMgrInfo *CargoMgrInfo
}

type CargoComm struct {
	cargoToMgr.UnimplementedRpcCargoToMgrServer

	cargoMgrInfo *CargoMgrInfo
}

type CargoNode struct {
	IP    string
	Port  string
	ID    string // <geohash + uuid>
	Lat   float64
	Lon   float64
	TSize float64
	RSize float64
}

type CargoMgrInfo struct {
	Port   string
	CC     CargoComm
	TCM    TaskComm
	Cargos map[string]CargoNode
}

func Init(port string) *CargoMgrInfo {
	var cargoMgrInfo CargoMgrInfo
	cargoMgrInfo.Port = port
	cargoMgrInfo.CC.cargoMgrInfo = &cargoMgrInfo
	cargoMgrInfo.TCM.cargoMgrInfo = &cargoMgrInfo
	cargoMgrInfo.Cargos = make(map[string]CargoNode)

	//fmt.Fprintf(os.Stderr, "Port number %s", cargoMgrInfo.Port)

	return &cargoMgrInfo
}

func (cc *CargoComm) RegisterToMgr(ctx context.Context, cargoInfo *cargoToMgr.CargoInfo) (*cargoToMgr.Ack, error) {
	newCargoNode := CargoNode{
		IP:    cargoInfo.GetIP(),
		Port:  cargoInfo.GetPort(),
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

func proximityComparison(ghSrc, ghDst []rune) int {
	ghSrcLen := len(ghSrc)

	prefixMatchCount := 0

	for i := 0; i < ghSrcLen; i++ {
		if ghSrc[i] == ghDst[i] {
			prefixMatchCount++
		} else {
			break
		}
	}
	return prefixMatchCount
}

type sortedNeighbors struct {
	hash string
	dist int
}

func (cargoMgrInfo *CargoMgrInfo) reportNeighborsInOrder(gh string, k int64) []string {
	nCargos := len(cargoMgrInfo.Cargos)
	SN := make([]sortedNeighbors, nCargos)
	idx := 0
	for key := range cargoMgrInfo.Cargos {
		result := strings.SplitN(key, "-", 2)
		SN[idx].hash = key
		SN[idx].dist = proximityComparison([]rune(gh), []rune(result[0]))
		idx++
	}
	sort.Slice(SN, func(i, j int) bool {
		return SN[i].dist < SN[j].dist
	})

	nearestNeighbors := make([]string, k)
	for i := 0; i < int(k) && i < len(SN); i++ {
		nearestNeighbors[i] = SN[i].hash
	}

	return nearestNeighbors
}

func (tcm *TaskComm) RequestCargo(ctx context.Context, requesterInfo *taskToCargoMgr.RequesterInfo) (*taskToCargoMgr.Cargos, error) {
	lat := requesterInfo.GetLat()
	lon := requesterInfo.GetLon()
	requesterGeoHash := geohash.Encode(lat, lon)

	nReplicas := requesterInfo.GetNReplicas()

	requestedCargos := tcm.cargoMgrInfo.reportNeighborsInOrder(requesterGeoHash, nReplicas)

	returnCargos := ""

	for i := 0; i < len(requestedCargos); i++ {
		hash := requestedCargos[i]
		if i == 0 {
			returnCargos = tcm.cargoMgrInfo.Cargos[hash].IP + ":" + tcm.cargoMgrInfo.Cargos[hash].Port
		} else {
			returnCargos = returnCargos + "#" + tcm.cargoMgrInfo.Cargos[hash].IP + ":" + tcm.cargoMgrInfo.Cargos[hash].Port
		}

	}

	return &taskToCargoMgr.Cargos{IPPort: returnCargos}, nil
}

func (cargoMgrInfo *CargoMgrInfo) ListenRoutine(wg *sync.WaitGroup) {
	defer wg.Done()

	listen, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", cargoMgrInfo.Port))
	cmd.CheckError(err)

	server := grpc.NewServer()
	cargoToMgr.RegisterRpcCargoToMgrServer(server, &(cargoMgrInfo.CC))
	taskToCargoMgr.RegisterRpcTaskToCargoMgrServer(server, &(cargoMgrInfo.TCM))

	reflection.Register(server)

	err = server.Serve(listen)
	cmd.CheckError(err)
}

// func (cargoMgrInfo *CargoMgrInfo) ListenTaskToMgr(wg *sync.WaitGroup) {
// 	defer wg.Done()

// 	listen, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", cargoMgrInfo.Port))
// 	cmd.CheckError(err)

// 	server := grpc.NewServer()
// 	cargoToMgr.RegisterRpcCargoToMgrServer(server, &(cargoMgrInfo.CC))
// 	taskToCargoMgr.RegisterRpcTaskToCargoMgrServer(server, &(cargoMgrInfo.TCM))

// 	reflection.Register(server)

// 	err = server.Serve(listen)
// 	cmd.CheckError(err)
// }
