package db

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/oklog/ulid/v2"
	bolt "go.etcd.io/bbolt"
)

type ConfigurationItem struct {
	ID      string                `json:"id"`
	Version int                   `json:"version"`
	Data    map[string]string     `json:"data"`
	Type    ConfigurationItemType `json:"type"`
}

type ConfigurationItemType struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (db *DB) AddCI(data map[string]string) (*ConfigurationItem, error) {
	normalizedData := normalizeData(data)

	datahash := hashData(normalizedData)

	cit, err := db.createOrGetCIType(datahash)
	if err != nil {
		return nil, err
	}

	ci := ConfigurationItem{
		ID:      ulid.Make().String(),
		Version: 0,
		Data:    data,
		Type:    *cit,
	}
	err = db.writeCI(ci)
	if err != nil {
		return nil, err
	}

	return &ci, nil
}

func (db *DB) GetCI(id string) (ConfigurationItem, error) {
	var ci ConfigurationItem

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("cis"))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		ciBytes := b.Get([]byte(id))
		if ciBytes == nil {
			return ErrKeyNotFound
		}

		return json.Unmarshal(ciBytes, &ci)
	})

	return ci, err
}

func (db *DB) GetCis() ([]ConfigurationItem, error) {
	var cis []ConfigurationItem

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("nodes"))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		return b.ForEach(func(k, v []byte) error {
			var ci ConfigurationItem
			err := json.Unmarshal(v, &ci)
			if err != nil {
				return err
			}

			cis = append(cis, ci)
			return nil
		})
	})

	return cis, err
}

func (db *DB) GetCIsByType(id string) ([]ConfigurationItem, error) {
	cis := []ConfigurationItem{}

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("cisbytype"))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		c := b.Cursor()

		prefix := []byte(id)
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			ci, err := db.GetCI(string(k))
			if err != nil {
				return err
			}
			cis = append(cis, ci)
		}

		return nil
	})

	return cis, err
}

// Replace the labels on a node with the provided labels.
func (db *DB) UpdateCI(id string, data map[string]string) error {
	normalizedData := normalizeData(data)
	dataHash := hashData(normalizedData)

	ci, err := db.GetCI(id)
	if err != nil {
		return err
	}

	cit, err := db.createOrGetCIType(dataHash)
	if err != nil {
		return err
	}

	ci.Data = normalizedData
	ci.Type = *cit

	return db.writeCI(ci)
}

// Delete the CI and all the relationships connected to it.
func (db *DB) DeleteCI(id string) error {
	rels, err := db.GetRelationshipsByCI(id)
	if err != nil {
		return err
	}
	for _, rel := range rels {
		err := db.DeleteRelationship(rel.ID)
		if err != nil {
			return err
		}
	}

	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("cis"))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		return b.Delete([]byte(id))
	})
}

// Get all CI types
func (db *DB) GetCITypes() ([]ConfigurationItemType, error) {
	var cits []ConfigurationItemType

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("citypes"))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		return b.ForEach(func(k, v []byte) error {
			cit := ConfigurationItemType{
				ID:   string(k),
				Name: string(v),
			}
			cits = append(cits, cit)
			return nil
		})
	})

	return cits, err
}

// Update name on CI type
func (db *DB) UpdateCITypeName(id string, name string) error {
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("citypes"))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		return b.Put([]byte(id), []byte(name))
	})
}

// Downcase all data labels and sort them alphabetically.
// This is to ensure that the same data labels will always hash to the same
// value.
func normalizeData(data map[string]string) map[string]string {
	// Downcase all the keys of the data map.
	normalizedData := make(map[string]string)
	for k, v := range data {
		normalizedData[strings.ToLower(k)] = v
	}

	// Sort the keys of the normalized data map.
	var keys []string
	for k := range normalizedData {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create a new map with the sorted keys.
	sortedAndNormalizedData := make(map[string]string)
	for _, k := range keys {
		sortedAndNormalizedData[k] = normalizedData[k]
	}

	return sortedAndNormalizedData
}

// Take a CI and use the data labels to create a CI "type"
func hashData(data map[string]string) string {
	labelsString := ""
	for k := range data {
		labelsString += k
	}

	hash := md5.Sum([]byte(labelsString))
	return string(hash[:])
}

func (db *DB) createOrGetCIType(hash string) (*ConfigurationItemType, error) {
	cit := ConfigurationItemType{}
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("citypes"))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		ciTypeBytes := b.Get([]byte(hash))
		if ciTypeBytes == nil {
			err := b.Put([]byte(hash), nil)
			if err != nil {
				return err
			}
			cit.ID = hash
		} else {
			cit.ID = hash
			cit.Name = string(ciTypeBytes)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &cit, nil
}

func (db *DB) writeCI(ci ConfigurationItem) error {
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("cis"))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		cbt := tx.Bucket([]byte("cisbytype"))
		if cbt == nil {
			return bolt.ErrBucketNotFound
		}

		ciBytes, err := json.Marshal(ci)
		if err != nil {
			return err
		}

		err = cbt.Put([]byte(fmt.Sprintf("%s:%s", ci.Type.ID, ci.ID)), nil)
		if err != nil {
			return err
		}

		return b.Put([]byte(ci.ID), ciBytes)
	})
}
