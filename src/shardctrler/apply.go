package shardctrler

import "sort"

func (sc *ShardCtrler) applyJoin(op Op) {
	// Read the previous configuration
	prevConfig := sc.configs[len(sc.configs)-1]

	// Create a new configuration based on the previous one
	newConfig := Config{
		Num:    prevConfig.Num + 1, // Increment config number
		Shards: prevConfig.Shards,  // Copy existing shard mappings
		Groups: make(map[int][]string),
	}

	// Copy existing groups to the new configuration
	for gid, servers := range prevConfig.Groups {
		newConfig.Groups[gid] = servers
	}

	// Add servers for the new GIDs
	for gid, servers := range op.Servers {
		newConfig.Groups[gid] = servers
	}

	// Calculate shard distribution among groups
	assignShards(&newConfig)

	// Append the new configuration to the list of configurations
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) applyLeave(op Op) {
	// Read the previous configuration
	prevConfig := sc.configs[len(sc.configs)-1]

	// Create a new configuration based on the previous one
	newConfig := Config{
		Num:    prevConfig.Num + 1, // Increment config number
		Shards: prevConfig.Shards,  // Copy existing shard mappings
		Groups: make(map[int][]string),
	}

	exclusion := createSet(op.GIDs)

	// Copy existing groups to the new configuration
	for gid, servers := range prevConfig.Groups {
		if !(exclusion[gid]) {
			newConfig.Groups[gid] = servers
		}
	}

	// Calculate shard distribution among groups
	assignShards(&newConfig)

	// Append the new configuration to the list of configurations
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) applyMove(op Op) {
	// Read the previous configuration
	prevConfig := sc.configs[len(sc.configs)-1]

	// Create a new configuration based on the previous one
	newConfig := Config{
		Num:    prevConfig.Num + 1, // Increment config number
		Shards: prevConfig.Shards,  // Copy existing shard mappings
		Groups: make(map[int][]string),
	}

	// Copy existing groups to the new configuration
	for gid, servers := range prevConfig.Groups {
		newConfig.Groups[gid] = servers
	}

	// Update shard
	newConfig.Shards[op.Shard] = op.GID
	sc.configs = append(sc.configs, newConfig)
}


// assignShards assigns shards to groups in a configuration.
func assignShards(config *Config) {
	// Calculate the number of shards per group
	numShards := len(config.Shards)
	numGroups := len(config.Groups)

	if numGroups == 0 {
        return // No groups, nothing to assign
    }

	// Collect group IDs and sort them
	groupIDs := make([]int, 0, len(config.Groups))
	for gid := range config.Groups {
		groupIDs = append(groupIDs, gid)
	}
	sort.Ints(groupIDs)

	shardsPerGroup := numShards / numGroups
	extraShards := numShards % numGroups

	// Initialize shard counter and group index
	shardCounter := 0
	groupIndex := 0

   	// Assign shards to groups
   	for shard := 0; shard < numShards; shard++ {
		config.Shards[shard] = groupIDs[groupIndex]

		shardCounter++
		if shardCounter >= shardsPerGroup + boolToInt(extraShards > 0) {
			// Deduct an extra shard if we're still distributing them
			if extraShards > 0 {
				extraShards--
			}

			// Move to the next group
			groupIndex++
			shardCounter = 0
		}
	}
}