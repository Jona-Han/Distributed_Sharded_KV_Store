package kvsRPC

import (
    "cpsc416/labrpc"
    "fmt"
)

type LabRPCClient struct {
    client *labrpc.ClientEnd
}

func NewLabRPCClient(client *labrpc.ClientEnd) *LabRPCClient {
    return &LabRPCClient{client: client}
}

func (c *LabRPCClient) Call(serviceMethod string, args interface{}, reply interface{}) (bool, error) {
    ok := c.client.Call(serviceMethod, args, reply)
    if !ok {
        return false, fmt.Errorf("RPC call to %s failed", serviceMethod)
    }
    return true, nil
}
