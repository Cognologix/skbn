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

	/*TODO: Update the logic
	  Keep only one switch case for src and check destination for each case
	*/
	switch srcPrefix {
	//case "k8s":
	case "s3":
	//case "abs":
	//case "gcs":
	default:
		return fmt.Errorf(srcPrefix + " not implemented")
	}

	switch dstPrefix {
	case "k8s":
	//case "s3":
	//case "abs":
	//case "gcs":
	default:
		return fmt.Errorf(dstPrefix + " not implemented")
	}

	return nil
}

//PerformSync performs actual sync operation
func PerformSync(srcClient, dstClient interface{}, srcPrefix, dstPrefix, srcPath, dstPath string,
	parallel int, bufferSize float64) error {

	//Get all files from source
	srcFileNames, err := GetFileNames(srcClient, srcPrefix, srcPath)
	if err != nil {
		return err
	}

	//Get all files from destination
	dstFileNames, err := GetFileNames(dstClient, dstPrefix, dstPath)
	if err != nil {
		return err
	}

	//find file intersection
	fileIntersection := utils.Intersection(srcFileNames, dstFileNames)

	//Find file which needs to be copied from source to destination
	copyFileNames := utils.Outersection(srcFileNames, fileIntersection)
	copyFilePaths := GetFromToPaths(srcPath, dstPath, copyFileNames)

	//Find file which needs to be deleted from destination
	delFileNames := utils.Outersection(dstFileNames, fileIntersection)
	delFilePaths := GetPaths(dstPath, delFileNames)

	//Copy files
	err = PerformCopy(srcClient, dstClient, srcPrefix, dstPrefix, copyFilePaths, parallel, bufferSize)
	if err != nil {
		return err
	}

	//Delete files
	err = PerformDelete(dstClient, dstPrefix, delFilePaths, parallel)
	if err != nil {
		return err
	}

	//Check sum to verify source and destination are in sync

	return nil
}
