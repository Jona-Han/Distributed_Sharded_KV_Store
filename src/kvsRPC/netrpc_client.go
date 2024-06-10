package kvsRPC

import "net/rpc"

type NetRPCClient struct {
	client *rpc.Client
}

func NewNetRPCClient(address string) (*NetRPCClient, error) {
    client, err := rpc.Dial("tcp", address)
    if err != nil {
        return nil, err
    }
    return &NetRPCClient{client: client}, nil
}

func (c *NetRPCClient) Call(serviceMethod string, args interface{}, reply interface{}) (bool, error) {
    err := c.client.Call(serviceMethod, args, reply)
    if err != nil {
        return false, err
    }
    return true, nil
}