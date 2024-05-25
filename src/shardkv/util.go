package shardkv
import (
	"cpsc416/shardctrler"
	"strconv"
)

func copyConfig(to *shardctrler.Config, from *shardctrler.Config) {
	to.Num = from.Num
	to.Groups = make(map[int][]string)
	for i, sh := range from.Shards {
		to.Shards[i] = sh
	}
	for k, v := range from.Groups {
		to.Groups[k] = v
	}
}

func termPlusIndexToStr(term int, index int) string {
	return strconv.Itoa(term) + "." + strconv.Itoa(index)
}
