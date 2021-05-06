package skbn

import "path/filepath"

//GetFileNames returns all file nams present at path
func GetFileNames(client interface{}, prefix, path string) ([]string, error) {

	fileNames, err := GetListOfFiles(client, prefix, path)
	if err != nil {
		return nil, err
	}

	return fileNames, nil
}

//GetPaths returns absolute path for each file name
func GetPaths(path string, fileNames []string) []string {

	var filePaths []string
	for _, fileName := range fileNames {
		filePaths = append(filePaths, filepath.Join(path, fileName))
	}

	return filePaths
}

//GetFromToPaths returns list of pair of source and destination files to copy
func GetFromToPaths(srcPath, dstPath string, relativePaths []string) []FromToPair {

	var fromToPaths []FromToPair
	for _, relativePath := range relativePaths {
		fromPath := filepath.Join(srcPath, relativePath)
		toPath := filepath.Join(dstPath, relativePath)
		fromToPaths = append(fromToPaths, FromToPair{FromPath: fromPath, ToPath: toPath})
	}

	return fromToPaths
}
