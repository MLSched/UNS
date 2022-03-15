package utils

func StringSliceEquals(a []string, b []string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i, s := range a {
		if b[i] != s {
			return false
		}
	}
	return true
}

func IndexOf(s []string, target string) int {
	for i, str := range s {
		if str == target {
			return i
		}
	}
	return -1
}
