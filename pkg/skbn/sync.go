package skbn

import (
	"fmt"

	"github.com/Cognologix/skbn/pkg/utils"
)

// Sync copies files from src to dst whick are not present at sest
// and delete extra files from dest
func Sync(src, dst string, parallel int, bufferSize float64) error {
	srcPrefix, srcPath := utils.SplitInTwo(src, "://")
	dstPrefix, dstPath := utils.SplitInTwo(dst, "://")

	err := TestImplementationsExistForSync(srcPrefix, dstPrefix)
	if err != nil {
		return err
	}

	srcClient, dstClient, err := GetClients(srcPrefix, dstPrefix, srcPath, dstPath)
	if err != nil {
		return err
	}

	err = PerformSync(srcClient, dstClient, srcPrefix, dstPrefix, srcPath, dstPath, parallel, bufferSize)
	if err != nil {
		return err
	}

	return nil
}

// TestImplementationsExistForSync checks that implementations exist for the desired action
func TestImplementationsExistForSync(srcPrefix, dstPrefix string) error {
	//For now sync is allowed only from S3 to K8s
	switch srcPrefix {
	case K8S:
		if dstPrefix != S3 {
			return fmt.Errorf(srcPrefix + "-->" + dstPrefix + " not implemented")
		}
	case S3:
		if dstPrefix != K8S {
			return fmt.Errorf(srcPrefix + "-->" + dstPrefix + " not implemented")
		}
	//case "abs":
	//case "gcs":
	default:
		return fmt.Errorf(srcPrefix + "-->" + dstPrefix + " not implemented")
	}

	return nil
}

//PerformSync performs actual sync operation
func PerformSync(srcClient, dstClient interface{}, srcPrefix, dstPrefix, srcPath, dstPath string,
	parallel int, bufferSize float64) error {

	var copyFilePaths []FromToPair
	var deleteFilePaths []string
	var err error
	if srcPrefix == S3 && dstPrefix == K8S {
		copyFilePaths, deleteFilePaths, err = S3ToK8s(srcClient, dstClient,
			srcPath, dstPath, parallel, bufferSize)
		if err != nil {
			return err
		}
	} else if srcPrefix == K8S && dstPrefix == S3 {
		copyFilePaths, deleteFilePaths, err = K8sToS3(srcClient, dstClient,
			srcPath, dstPath, parallel, bufferSize)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf(srcPrefix + "-->" + dstPrefix + " not implemented")
	}

	//Copy files
	err = PerformCopy(srcClient, dstClient, srcPrefix, dstPrefix, copyFilePaths, parallel, bufferSize)
	if err != nil {
		return err
	}

	//Delete files
	err = PerformDelete(dstClient, dstPrefix, deleteFilePaths, parallel)
	if err != nil {
		return err
	}

	return nil
}

func S3ToK8s(srcClient, dstClient interface{}, srcPath, dstPath string,
	parallel int, bufferSize float64) ([]FromToPair, []string, error) {

	srcFileObjs, err := GetListOfFilesFromS3V2(srcClient, srcPath)
	if err != nil {
		return nil, nil, err
	}

	dstFileObjs, err := GetListOfFilesFromK8sV2(dstClient, dstPath, "f", "*")
	if err != nil {
		return nil, nil, err
	}

	err = SetFileETag(dstClient, dstPath, dstFileObjs)
	if err != nil {
		return nil, nil, err
	}

	var copyFileNames []string
	var deleteFileNames []string

	//Collect files to copy
	for _, srcFile := range srcFileObjs {
		if _, ok := dstFileObjs[srcFile.name]; !ok {
			fmt.Println("Checking by file name")
			copyFileNames = append(copyFileNames, srcFile.name)
		} else {
			if srcFile.eTag != dstFileObjs[srcFile.name].eTag {
				copyFileNames = append(copyFileNames, srcFile.name)
			}
		}
	}

	//collect extra files to delete from destination
	for _, dstFile := range dstFileObjs {
		if _, ok := srcFileObjs[dstFile.name]; !ok {
			deleteFileNames = append(deleteFileNames, dstFile.name)
		}
	}
	copyFilePaths := GetFromToPaths(srcPath, dstPath, copyFileNames)
	deleteFilePaths := GetPaths(dstPath, deleteFileNames)

	return copyFilePaths, deleteFilePaths, nil
}

func K8sToS3(srcClient, dstClient interface{}, srcPath, dstPath string,
	parallel int, bufferSize float64) ([]FromToPair, []string, error) {

	srcFileObjs, err := GetListOfFilesFromK8sV2(srcClient, srcPath, "f", "*")
	if err != nil {
		return nil, nil, err
	}

	dstFileObjs, err := GetListOfFilesFromS3V2(dstClient, dstPath)
	if err != nil {
		return nil, nil, err
	}

	err = SetFileETag(srcClient, srcPath, srcFileObjs)
	if err != nil {
		return nil, nil, err
	}

	var copyFileNames []string
	var deleteFileNames []string

	//Collect files to copy
	for _, srcFile := range srcFileObjs {
		if _, ok := dstFileObjs[srcFile.name]; !ok {
			fmt.Println("Checking by file name")
			copyFileNames = append(copyFileNames, srcFile.name)
		} else {
			if srcFile.eTag != dstFileObjs[srcFile.name].eTag {
				copyFileNames = append(copyFileNames, srcFile.name)
			}
		}
	}

	//collect extra files to delete from destination
	for _, dstFile := range dstFileObjs {
		if _, ok := srcFileObjs[dstFile.name]; !ok {
			deleteFileNames = append(deleteFileNames, dstFile.name)
		}
	}
	copyFilePaths := GetFromToPaths(srcPath, dstPath, copyFileNames)
	deleteFilePaths := GetPaths(dstPath, deleteFileNames)

	return copyFilePaths, deleteFilePaths, nil
}
