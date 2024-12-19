package blobster

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

var url string = "blob.core.windows.net/"

func AzureUpload(account, container, dir string) error {
	fmt.Println("In the export function")
	c, _ := getBlobClient(account)
	listBlob(c, container)
	err := uploadDirFiles(c, container, dir)
	checkErr(err)
	return nil
}

func getBlobClient(account string) (*azblob.Client, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	fmt.Println("authenticating to Azure...")
	if err != nil {
		log.Fatalf("authentication to azure failed \n %v \n", err)
	}

	client, err := azblob.NewClient(fmt.Sprintf("https://%s.%s", account, url), cred, nil)
	checkErr(err)

	return client, nil
}

func listBlob(c *azblob.Client, container string) {
	pager := c.NewListBlobsFlatPager(container, nil)

	for pager.More() {
		page, err := pager.NextPage(context.TODO())

		checkErr(err)

		for _, blob := range page.Segment.BlobItems {
			fmt.Println(*blob.Name)
		}
	}
}

func uploadDirFiles(c *azblob.Client, blob, dir string) error {
	// Check dir that dir is directory and not empty
	// Iterate through files and upload one at a time
	// This could be recursive to upload dirs in dirs
	d, err := os.ReadDir(dir)
	checkErr(err)
	for _, entry := range d {
		if !entry.IsDir() {
			fmt.Println(entry.Name())
			f := fmt.Sprintf("%s/%s", dir, entry.Name())
			file, err := os.OpenFile(f, os.O_RDONLY, 0)
			checkErr(err)
			defer file.Close()
			fmt.Println(blob)
			_, err = c.UploadFile(context.TODO(), blob, entry.Name(), file, nil)
			checkErr(err)

		}
	}
	return nil
}

func checkErr(err error) {
	if err != nil {
		log.Fatalf("an error occurred: %v \n", err)
	}
}
