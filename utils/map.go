package utils

import mapset "github.com/deckarep/golang-set"

func CloneStringSet(set map[string]bool) map[string]bool {
	s := make(map[string]bool)
	for k := range set {
		s[k] = true
	}
	return s
}

func MapSet2StringSlice(set mapset.Set) []string {
	slice := set.ToSlice()
	r := make([]string, 0, len(slice))
	for _, s := range slice {
		r = append(r, s.(string))
	}
	return r
}
