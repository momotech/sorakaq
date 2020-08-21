package util

import (
	"fmt"
	"os"
)

func OpenFile(fileName string) *os.File {
	logFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic(fmt.Sprintf("Open file Failed: %s", fileName))
	}
	return logFile
}
