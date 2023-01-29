package mr

import (
	"fmt"
	"os"
)

// 清除可能存在的文件
func mayClearFile(path string) error {
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		err := os.RemoveAll(path)
		if err != nil {
			return err
		}
	}
	return nil
}

func clearAndMakeNewDir(path string) error {
	err := mayClearFile(path)
	if err != nil {
		return err
	}
	err = os.Mkdir(path, os.ModePerm)
	if err != nil {
		return err
	}
	return nil
}

func getOutputFileName(reduceTaskNum int) string {
	return fmt.Sprintf("mr-out-%d", reduceTaskNum)
}
