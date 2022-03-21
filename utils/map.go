package utils

func CloneStringSet(set map[string]bool) map[string]bool {
	s := make(map[string]bool)
	for k := range set {
		s[k] = true
	}
	return s
}
