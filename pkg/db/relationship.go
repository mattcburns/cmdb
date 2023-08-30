package db

import (
	"encoding/json"
	"fmt"
	"strings"

	bolt "go.etcd.io/bbolt"
)

type Relationship struct {
	ID           string            `json:"id"`
	From         string            `json:"from"`
	To           string            `json:"to"`
	Relationship string            `json:"relationship"`
	Version      int               `json:"version"`
	Data         map[string]string `json:"data"`
}

func (db *DB) AddRelationship(from string, to string, relationship string, data map[string]string) (*Relationship, error) {
	err := guardName(relationship)
	if err != nil {
		return nil, err
	}

	relationship = normalizeRelationshipName(relationship)

	rel := Relationship{
		ID:           fmt.Sprintf("%s:%s", from, to),
		From:         from,
		To:           to,
		Relationship: relationship,
		Version:      0,
		Data:         data,
	}

	err = db.writeRel(rel)
	if err != nil {
		return nil, err
	}

	return &rel, nil
}

func (db *DB) GetRelationship(id string) (*Relationship, error) {
	var rel Relationship

	relbucket, err := db.getRelationshipBucket(id)
	if err != nil {
		return nil, err
	}

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(*relbucket))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		edgeBytes := b.Get([]byte(id))
		if edgeBytes == nil {
			return ErrKeyNotFound
		}

		return json.Unmarshal(edgeBytes, &rel)
	})

	return &rel, err
}

// Get all relationship types.
func (db *DB) GetRelationshipTypes() ([]string, error) {
	var rels []string

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("reltypes"))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		return b.ForEach(func(k, v []byte) error {
			rels = append(rels, string(k))
			return nil
		})

	})

	return rels, err
}

// Get all relationships of a given type.
func (db *DB) GetRelationshipsByType(relationship string) ([]Relationship, error) {
	var rels []Relationship

	relationship = normalizeRelationshipName(relationship)

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(relationship))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		return b.ForEach(func(k, v []byte) error {
			var rel Relationship
			err := json.Unmarshal(v, &rel)
			if err != nil {
				return err
			}

			rels = append(rels, rel)
			return nil
		})
	})

	return rels, err
}

// Get all relationships attached to a given node.
func (db *DB) GetRelationshipsByCI(id string) ([]Relationship, error) {
	rels := []Relationship{}

	err := db.View(func(tx *bolt.Tx) error {
		ob := tx.Bucket([]byte("relsoutgoing"))
		if ob == nil {
			return bolt.ErrBucketNotFound
		}

		ib := tx.Bucket([]byte("relsincoming"))
		if ib == nil {
			return bolt.ErrBucketNotFound
		}

		for k, v := ob.Cursor().Seek([]byte(id + ":")); strings.HasPrefix(string(k), id+":"); k, v = ob.Cursor().Next() {
			var rel Relationship
			err := json.Unmarshal(v, &rel)
			if err != nil {
				return err
			}

			rels = append(rels, rel)
		}

		for k, v := ib.Cursor().Seek([]byte(id + ":")); strings.HasPrefix(string(k), id+":"); k, v = ib.Cursor().Next() {
			var rel Relationship
			err := json.Unmarshal(v, &rel)
			if err != nil {
				return err
			}

			rels = append(rels, rel)
		}

		return nil
	})

	return rels, err
}

func (db *DB) GetRelationships() ([]Relationship, error) {
	var rels []Relationship

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("rels"))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		return b.ForEach(func(k, v []byte) error {
			var rel Relationship
			err := json.Unmarshal(v, &rel)
			if err != nil {
				return err
			}

			rels = append(rels, rel)
			return nil
		})
	})

	return rels, err
}

func (db *DB) DeleteRelationship(id string) error {
	return db.Update(func(tx *bolt.Tx) error {
		relbucket, err := db.getRelationshipBucket(id)
		if err != nil {
			return err
		}

		rels := tx.Bucket([]byte(*relbucket))

		relbytes := rels.Get([]byte(id))

		var rel Relationship
		err = json.Unmarshal(relbytes, &rel)
		if err != nil {
			return err
		}

		rti := tx.Bucket([]byte("reltypeindex"))
		if rti == nil {
			return bolt.ErrBucketNotFound
		}

		err = rti.Delete([]byte(id))
		if err != nil {
			return err
		}

		relinc := tx.Bucket([]byte("relsincoming"))
		if relinc == nil {
			return bolt.ErrBucketNotFound
		}

		err = relinc.Delete([]byte(fmt.Sprintf("%s:%s", rel.To, rel.From)))
		if err != nil {
			return err
		}

		relout := tx.Bucket([]byte("relsoutgoing"))
		if relout == nil {
			return bolt.ErrBucketNotFound
		}

		err = relout.Delete([]byte(fmt.Sprintf("%s:%s", rel.From, rel.To)))
		if err != nil {
			return err
		}

		return rels.Delete([]byte(id))

	})
}

func (db *DB) UpdateRelationship(id string, data map[string]string) (*Relationship, error) {
	rel, err := db.GetRelationship(id)
	if err != nil {
		return nil, err
	}

	rel.Version++
	rel.Data = data

	err = db.writeRel(*rel)
	if err != nil {
		return nil, err
	}

	return rel, nil
}

func normalizeRelationshipName(relationship string) string {
	return strings.ToLower(relationship)
}

func (db *DB) getRelationshipBucket(relID string) (*string, error) {
	var bucket string

	db.View(func(tx *bolt.Tx) error {
		m := tx.Bucket([]byte("rels"))
		if m == nil {
			return bolt.ErrBucketNotFound
		}

		bucket = string(m.Get([]byte("index:" + relID)))

		return nil
	})

	if bucket == "" {
		return nil, ErrKeyNotFound
	}

	return &bucket, nil
}

func (db *DB) writeRel(rel Relationship) error {
	rel.Relationship = normalizeRelationshipName(rel.Relationship)

	relBytes, err := json.Marshal(rel)
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(rel.Relationship))
		if err != nil {
			return err
		}

		// This section is for metadata.
		// Create an index of all the bucket labels so we can look them up.
		rt := tx.Bucket([]byte("reltypes"))
		err = rt.Put([]byte(rel.Relationship), nil)
		if err != nil {
			return err
		}
		// Create an index of all the relationships organized by ID so we can
		// find the bucket the relationship is in.\
		rti := tx.Bucket([]byte("reltypeindex"))
		err = rti.Put([]byte(rel.ID), []byte(rel.Relationship))
		if err != nil {
			return err
		}

		// The following two methods get complicated to reason about, so let's
		// make it simpler:
		// to:from is incoming. When I want to know all the incoming <-
		// relationships to a CI. (Who is connecting to me?)
		// from:to is outgoing. When I want to know all the outgoing ->
		// relationships from a CI. (Who am I connecting to?)

		// Incoming relationship index. to:from is the ID format.
		relinc := tx.Bucket([]byte("relsincoming"))
		err = relinc.Put([]byte(fmt.Sprintf("%s:%s", rel.To, rel.From)), relBytes)
		if err != nil {
			return err
		}

		// Outgoing relationship index. from:to is the ID format.
		relout := tx.Bucket([]byte("relsoutgoing"))
		err = relout.Put([]byte(fmt.Sprintf("%s:%s", rel.From, rel.To)), relBytes)
		if err != nil {
			return err
		}

		// Finish up by storing the relationship in it's bucket.
		return b.Put([]byte(rel.ID), relBytes)
	})
}
