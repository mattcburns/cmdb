package main

import (
	"log"

	"github.com/mattcburns/cmdb/pkg/db"
)

func main() {
	cmdb, err := db.NewDB("cmdb.db")

	if err != nil {
		log.Fatal(err)
	}

	defer cmdb.Close()

	// Add a CI
	ci, err := cmdb.AddCI(map[string]string{
		"hostname": "web01",
		"ip":       "192.168.0.100",
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Added CI: %+v\n", ci)

	// Update the citype name
	err = cmdb.UpdateCITypeName(ci.Type.ID, "webserver")
	if err != nil {
		log.Fatal(err)
	}

	// Add a CI
	ci2, err := cmdb.AddCI(map[string]string{
		"hostname": "web02",
		"ip":       "192.168.0.101",
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Added CI: %+v\n", ci2)

	// Create a relationship
	rel, err := cmdb.AddRelationship(ci.ID, ci2.ID, "connected-to", map[string]string{
		"master": "web02",
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Added Relationship: %+v\n", rel)

	// List all rels connected to ci
	rels, err := cmdb.GetRelationshipsByCI(ci.ID)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Length of rels: %d", len(rels))

	for i := 0; i < len(rels); i++ {
		log.Printf("Rel: %+v\n", rels[i])
	}

}
