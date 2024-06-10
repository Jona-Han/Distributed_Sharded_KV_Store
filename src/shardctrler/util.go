/*
Package shardctrler provides mechanisms to manage shard configurations in a distributed system.
It allows joining new groups, leaving groups, and moving shards between groups.
*/
package shardctrler

// createSet creates a set from a slice of GIDs.
// returns a map of shard values to boolean values
func createSet(nums []int) map[int]bool {
    set := make(map[int]bool)
    for _, num := range nums {
        set[num] = true
    }
    return set
}

// boolToInt converts a boolean to an integer 
// return 1 if true, 0 if false).
func boolToInt(b bool) int {
    if b {
        return 1
    }
    return 0
}