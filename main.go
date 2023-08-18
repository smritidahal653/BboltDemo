package main

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"math/rand"
	"os"
	"time"

	"go.etcd.io/bbolt"
)

type Content struct {
	Title       string
	Createdtime time.Time
}

const (
	bucketPrefix = "bucket"
	keyPrefix    = "key"
	srcDbPath    = "./data.db"
	dstDbPath    = "./newData.db"
	numItems     = 1000000
	numBuckets   = 500
	txPerBucket  = numItems / numBuckets
)

func insertObjectIntoBucket(tx *bbolt.Tx, b int) error {
	bucket, err := tx.CreateBucketIfNotExists(bucketName(b))
	if err != nil {
		return err
	}

	// Store 1 million objects with random data in each bucket
	for i := 0; i < txPerBucket; i++ {
		key, value, err := generateKeyValue(b, i)
		if err != nil {
			return err
		}

		if err = bucket.Put(key, value); err != nil {
			return err
		}
	}

	return nil
}

func deleteObjectsFromBucket(tx *bbolt.Tx) error {
	randBucketIndex := randInt(0, numBuckets-1)
	bucketName := fmt.Sprintf("bucket_%d", randBucketIndex)
	bucket := tx.Bucket([]byte(bucketName))

	if bucket == nil {
		log.Printf("Bucket '%s' not found\n", bucketName)
	}

	randKeyIndex := randInt(0, txPerBucket)
	key := []byte(fmt.Sprintf("%s_%d_%d", keyPrefix, randBucketIndex, randKeyIndex))

	if err := bucket.Delete(key); err != nil {
		return err
	}
	return nil
}

func generateKeyValue(bucketIndex int, keyIndex int) ([]byte, []byte, error) {
	key := []byte(fmt.Sprintf("%s_%d_%d", keyPrefix, bucketIndex, keyIndex))

	obj := &Content{
		Title:       fmt.Sprintf("content%d", keyIndex),
		Createdtime: time.Now(),
	}

	value, err := json.Marshal(obj)
	if err != nil {
		return nil, nil, err
	}

	return key, value, nil
}

func bucketName(index int) []byte {
	bucket := fmt.Sprintf("%s_%d", bucketPrefix, index)
	return []byte(bucket)
}

func randInt(min, max int) int {
	return min + rand.Intn(max-min)
}

func getFileInfo(filePath string) fs.FileInfo {
	fi, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		log.Fatal(err)
	} else if err != nil {
		log.Fatal(err)
	}
	return fi
}

func main() {
	// Open or create the database
	db, err := bbolt.Open(srcDbPath, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	startTime := time.Now()

	// Create multiple buckets
	for b := 0; b < numBuckets; b++ {
		err = db.Update(func(tx *bbolt.Tx) error {
			err = insertObjectIntoBucket(tx, b)
			return nil
		})

		if err != nil {
			log.Fatal(err)
		}
	}

	elapsedTime := time.Since(startTime)
	fmt.Printf("Stored %d objects in the database in %v.\n", numItems, elapsedTime)

	//Ensure source file exists
	fi := getFileInfo(srcDbPath)

	sizeAfterStorage := fi.Size()
	log.Printf("File size after initial storage: %d", sizeAfterStorage)

	// Delete 5000 random objects in the database
	totalDeleted := 0
	for i := 0; i < 5000; i++ {
		err = db.Update(func(tx *bbolt.Tx) error {
			err = deleteObjectsFromBucket(tx)
			totalDeleted++
			return nil
		})

		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Printf("Deleted %d random objects from the database.\n", totalDeleted)
	fi = getFileInfo(srcDbPath)
	sizeAfterDeletion := fi.Size()
	log.Printf("File size after random deletion: %d", sizeAfterDeletion)

	dstDb, err := bbolt.Open(dstDbPath, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer dstDb.Close()

	// Run compaction.
	if err := bbolt.Compact(dstDb, db, 0); err != nil {
		log.Fatal(err)
	}

	fi = getFileInfo(dstDbPath)
	sizeAfterCompaction := fi.Size()

	log.Printf("File size after compaction: %d", sizeAfterCompaction)

	//add back 5000 transactions into the first bucket
	err = dstDb.Update(func(tx *bbolt.Tx) error {

		for i := txPerBucket; i < txPerBucket+5000; i++ {
			b := randInt(0, numBuckets)
			bucket := tx.Bucket(bucketName(b))
			key, value, err := generateKeyValue(b, i)
			if err != nil {
				return err
			}

			if err := bucket.Put(key, value); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Stored %d new objects in the database.\n", totalDeleted)
	fi = getFileInfo(dstDbPath)
	sizeAfterNewAdditions := fi.Size()
	log.Printf("File size after new objects stored: %d", sizeAfterNewAdditions)
}
