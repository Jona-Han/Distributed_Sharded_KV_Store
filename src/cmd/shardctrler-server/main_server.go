package shardservermain

import (
	"fmt"
	"os"
	"net"
    "net/rpc"
    "strconv"
	"encoding/gob"
    "cpsc416/shardctrler"
	"cpsc416/raft"
	"cpsc416/kvsRPC"
)

// TODO: Add ServiceIP and server_list to compose
func main() {
    serviceName := os.Getenv("SERVICE_NAME")
    servicePort := os.Getenv("SERVICE_PORT")
	serverList := os.Getenv("SERVER_LIST")
    address := fmt.Sprintf(":%s", servicePort)

    fmt.Printf("Starting %s on port %s\n", serviceName, servicePort)

	servers := make([]kvsRPC.RPCClient, len(serverList))
	for idx, server := range serverList {
		servers[idx] = kvsRPC.NewLabRPCClient(server)
	}

    thisIndex = strvconv.Atoi(serviceName[len(serviceName) - 1])

	gob.Register(shardctrler.Op{})

    // Create a new instance of server
    srv := shardctrler.StartServer(servers, thisIndex, raft.MakePersister())

    // Register the server with net/rpc
    rpc.Register(srv)

    // Start listening for incoming connections
    listener, err := net.Listen("tcp", address)
    if err != nil {
        fmt.Println("Error starting server:", err)
        return
    }
	defer listener.Close()

    fmt.Printf("Server %s started on port %s\n", serviceName, servicePort)
	rpc.Accept(listener)
}