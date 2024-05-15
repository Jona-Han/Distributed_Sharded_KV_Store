package shardctrler

func createSet(nums []int) map[int]bool {
    set := make(map[int]bool)
    for _, num := range nums {
        set[num] = true
    }
    return set
}

func boolToInt(b bool) int {
    if b {
        return 1
    }
    return 0
}