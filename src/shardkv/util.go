package shardkv
import (
	"cpsc416/shardctrler"
	"fmt"
)

func copyConfig(to *shardctrler.Config, from *shardctrler.Config) {
	to.Num = from.Num
	to.Shards = from.Shards

	to.Groups = make(map[int][]string, len(from.Groups))
	for k, v := range from.Groups {
		newSlice := make([]string, len(v))
		copy(newSlice, v)
		to.Groups[k] = newSlice
	}
}

func termPlusIndexToStr(term int, index int) string {
	return fmt.Sprintf("%d+%d", term, index)
}
