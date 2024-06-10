package kvsRPC

// RPCClient is the interface for RPC calls
type RPCClient interface {
    Call(serviceMethod string, args interface{}, reply interface{}) (bool, error)
}
