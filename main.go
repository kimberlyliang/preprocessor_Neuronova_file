package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"

	"log/slog"
)

func main() {
	programLevel := new(slog.LevelVar)
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: programLevel}))
	slog.SetDefault(logger)

	integrationID := os.Getenv("INTEGRATION_ID")
	logger.Info(integrationID)
	inputDir := os.Getenv("INPUT_DIR")

	// get input files
	sessionToken := os.Getenv("SESSION_TOKEN")
	apiHost := os.Getenv("PENNSIEVE_API_HOST")
	apiHost2 := os.Getenv("PENNSIEVE_API_HOST2")
	integrationResponse, err := getIntegration(apiHost2, integrationID, sessionToken)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(string(integrationResponse))
	var integration Integration
	if err := json.Unmarshal(integrationResponse, &integration); err != nil {
		logger.ErrorContext(context.Background(), err.Error())
	}
	fmt.Println(integration)

	manifest, err := getPresignedUrls(apiHost, getPackageIds(integration.PackageIDs), sessionToken)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(string(manifest))
	var payload Manifest
	if err := json.Unmarshal(manifest, &payload); err != nil {
		logger.ErrorContext(context.Background(), err.Error())
	}

	// copy files into input directory
	fmt.Println(payload.Data)
	for _, d := range payload.Data {
		fmt.Printf("Original filename: %s\n", d.FileName)
		newFileName := extractSubIdentifier(d.FileName)
		fmt.Printf("Extracted sub-identifier: %s\n", newFileName)
		cmd := exec.Command("wget", "-v", "-O", newFileName, d.Url)
		cmd.Dir = inputDir
		var out strings.Builder
		var stderr strings.Builder
		cmd.Stdout = &out
		cmd.Stderr = &stderr
		err := cmd.Run()

		// Print stdout content
		stdoutContent := out.String()
		fmt.Println("Stdout output:")
		fmt.Println(stdoutContent)

		// Print or log stderr content
		stderrContent := stderr.String()
		fmt.Println("Stderr output (verbose output):")
		fmt.Println(stderrContent)

		// If there was an error, log it
		if err != nil {
			logger.Error(err.Error(),
				slog.String("error", stderrContent))
		}
	}

}

type Packages struct {
	NodeIds []string `json:"nodeIds"`
}

type Manifest struct {
	Data []ManifestData `json:"data"`
}

type ManifestData struct {
	NodeId   string   `json:"nodeId"`
	FileName string   `json:"fileName"`
	Path     []string `json:"path"`
	Url      string   `json:"url"`
}

type Integration struct {
	Uuid          string      `json:"uuid"`
	ApplicationID int64       `json:"applicationId"`
	DatasetNodeID string      `json:"datasetId"`
	PackageIDs    []string    `json:"packageIds"`
	Params        interface{} `json:"params"`
}

func getPresignedUrls(apiHost string, packages Packages, sessionToken string) ([]byte, error) {
	url := fmt.Sprintf("%s/packages/download-manifest?api_key=%s", apiHost, sessionToken)
	b, err := json.Marshal(packages)
	if err != nil {
		return nil, err
	}
	fmt.Println(string(b))

	payload := strings.NewReader(string(b))

	req, _ := http.NewRequest("POST", url, payload)

	req.Header.Add("accept", "*/*")
	req.Header.Add("content-type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	return body, nil
}

func getPackageIds(packageIds []string) Packages {
	return Packages{
		NodeIds: packageIds,
	}
}

func getIntegration(apiHost string, integrationId string, sessionToken string) ([]byte, error) {
	url := fmt.Sprintf("%s/integrations/%s", apiHost, integrationId)

	req, _ := http.NewRequest("GET", url, nil)

	req.Header.Add("accept", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", sessionToken))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	return body, nil
}

func extractSubIdentifier(fileName string) string {
	// Look for "sub-" in the filename
	if idx := strings.Index(fileName, "sub-"); idx != -1 {
		// Get everything after "sub-"
		afterSub := fileName[idx+4:]
		// Find the next separator (either "-" or ".")
		nextSep := strings.IndexAny(afterSub, "-.")
		if nextSep != -1 {
			// Return everything between "sub-" and the next separator
			return afterSub[:nextSep]
		}
		// If no separator found, return everything after "sub-"
		return afterSub
	}
	return fileName
}
