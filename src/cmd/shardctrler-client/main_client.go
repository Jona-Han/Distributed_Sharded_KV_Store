package shard

import (
	"fmt"
	"os"
	"cpsc416/labrpc"
	"cpsc416/server"
)

func main() {
	serviceName := os.Getenv("SERVICE_NAME")
	servicePort := os.Getenv("SERVICE_PORT")

	fmt.Printf("Starting %s on port %s\n", serviceName, servicePort)

	network := labrpc.MakeNetwork()
	srv := server.MakeServer() // Assuming MakeServer() is a function in server package
	network.AddServer(serviceName, srv)
}
