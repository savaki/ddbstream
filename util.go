package ddbstream

// containsString returns true if want is in the set, ss
func containsString(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
