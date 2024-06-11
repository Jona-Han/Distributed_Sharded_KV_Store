package shardclientmain

import (
	"fmt"
	"os"
)

func main() {
	serviceName := os.Getenv("SERVICE_NAME")
	servicePort := os.Getenv("SERVICE_PORT")

	fmt.Printf("Starting %s on port %s\n", serviceName, servicePort)
}
