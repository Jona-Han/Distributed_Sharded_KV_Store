package kvsRPC

import (
    "cpsc416/labrpc"
)

type LabRPCClient struct {
    client *labrpc.ClientEnd
}

func NewLabRPCClient(client *labrpc.ClientEnd) *LabRPCClient {
    return &LabRPCClient{client: client}
}

func (c *LabRPCClient) Call(serviceMethod string, args interface{}, reply interface{}) error {
    return c.client.Call(serviceMethod, args, reply)
}
