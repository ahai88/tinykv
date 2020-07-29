package standalone_storage

import (
	"fmt"
	"strings"
)

func createIndexKey(cf string, key string) string {
	return fmt.Sprintf("cf:%s:key:%s", cf, key)
}

func keyWithoutPrefix(storageKey string) string {
	ss := strings.Split(storageKey, ":")
	return ss[len(ss)-1]
}

func createCF(cf string) string {
	return fmt.Sprintf("cf:%s", cf)
}
