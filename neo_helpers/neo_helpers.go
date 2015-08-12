package neohelpers

import (
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/cihangir/neo4j"
)

var neoHelpers NeoHelpers

// NeoHelpers - Empty struct to access these helper methods
type NeoHelpers struct {
}

// UpdateNodeWithCypher - Update node with Cypher
func UpdateNodeWithCypher(neo4jConnection *neo4j.Neo4j, query string) ([]interface{}, error) {
	cypher := &neo4j.Cypher{
		Query: map[string]string{
			"query": query,
		},
		Payload: map[string]interface{}{},
	}

	batch := neo4jConnection.NewBatch()
	batch.Create(cypher)
	_, err := batch.Execute()
	if err != nil {
		log.Error("Cypher error: %v", err)
		return nil, err
	}
	if cypher.Payload.(map[string]interface{})["data"] == nil {
		log.Info("NO DATA FROM CYPHER")
		return nil, errors.New("NO DATA FROM CYPHER")
	}
	data, _ := cypher.Payload.(map[string]interface{})["data"].([]interface{})
	return data, nil

}

// FindUserByCypher - Finds User node by Cypher query
func FindUserByCypher(neo4jConnection *neo4j.Neo4j, query string) ([]interface{}, error) {
	cypher := &neo4j.Cypher{
		Query: map[string]string{
			"query": query,
		},
		Payload: map[string]interface{}{},
	}

	batch := neo4jConnection.NewBatch()
	batch.Create(cypher)
	_, err := batch.Execute()
	if err != nil {
		log.Error("Cypher error: %v", err)
		return nil, err
	}
	if cypher.Payload.(map[string]interface{})["data"] == nil {
		log.Info("NO DATA FROM CYPHER")
		return nil, errors.New("NO DATA FROM CYPHER")
	}
	data, _ := cypher.Payload.(map[string]interface{})["data"].([]interface{})
	return data, nil
}

// FindIDByCypher - Executes Cypher Query
// TODO Refactor - Use a struct to map the response from cypher query instead of casting slices
func FindIDByCypher(neo4jConnection *neo4j.Neo4j, query string) (int, error) {
	cypher := &neo4j.Cypher{
		Query: map[string]string{
			"query": query,
		},
		Payload: map[string]interface{}{},
	}

	batch := neo4jConnection.NewBatch()
	batch.Create(cypher)
	_, err := batch.Execute()
	if err != nil {
		log.Error("Cypher error: %v", err)
		return 0, err
	}

	if cypher.Payload.(map[string]interface{})["data"] == nil {
		log.Info("NO DATA FROM CYPHER")
		return 0, errors.New("NO DATA FROM CYPHER")
	}
	firstSlice, ok := cypher.Payload.(map[string]interface{})["data"].([]interface{})
	if !ok {
		log.Info("No Cypher data")
		return 0, errors.New("No Cypher data")
	}

	if len(firstSlice) < 1 {
		return 0, errors.New("Neo Venue Not found")
	}

	secondSlice, ok2 := firstSlice[0].([]interface{})
	if !ok2 {
		log.Info("Still No Cypher Data")
		return 0, errors.New("Still No Cypher Data")
	}
	thirdSlice, ok3 := secondSlice[0].(float64)
	if !ok3 {
		log.Info("Still No Cypher Data")
		return 0, errors.New("Still No Cypher Data")
	}
	log.Info("NEO ID TO RETURN ON SEARCH ", thirdSlice)
	return int(thirdSlice), nil
}

// CreateCypherLabelOperation - Use to create a Label in a Batch Operation
func CreateCypherLabelOperation(unique *neo4j.Unique, label string) *neo4j.Cypher {
	query := fmt.Sprintf("START n = node:%v(%v='%v') set n %v return n", unique.IndexName, unique.Key, unique.Value, label)
	cypher := &neo4j.Cypher{
		Query: map[string]string{
			"query": query,
		},
		Payload: map[string]interface{}{},
	}
	return cypher
}

// CreateCypherRelationshipOperationFromDifferentIndex - Used to create Relationship with a Person the User FOLLOWS
func CreateCypherRelationshipOperationFromDifferentIndex(fromUnique *neo4j.Unique, toUnique *neo4j.Unique, relName string) *neo4j.Cypher {
	query := fmt.Sprintf("START me = node:%v(%v='%v'), you = node:%v(%v='%v') CREATE UNIQUE me-[new_rel:%v]-> you RETURN new_rel",
		fromUnique.IndexName, fromUnique.Key, fromUnique.Value, toUnique.IndexName, toUnique.Key, toUnique.Value, relName)
	cypher := &neo4j.Cypher{
		Query: map[string]string{
			"query": query,
		},
		Payload: map[string]interface{}{},
	}
	return cypher
}

// CreateCypherRelationshipOperationFrom - Used to create Relationship with a Person the User FOLLOWS
func CreateCypherRelationshipOperationFrom(fromNodeIndexValue string, unique *neo4j.Unique, relName string) *neo4j.Cypher {
	query := fmt.Sprintf("START me = node:%v(%v='%v'), you = node:%v(%v='%v') CREATE UNIQUE me-[new_rel:%v]-> you RETURN new_rel",
		unique.IndexName, unique.Key, fromNodeIndexValue, unique.IndexName, unique.Key, unique.Value, relName)
	cypher := &neo4j.Cypher{
		Query: map[string]string{
			"query": query,
		},
		Payload: map[string]interface{}{},
	}
	return cypher
}

// CreateCypherRelationshipOperationTo - Used to create Relationship with a Person who is a FOLLOWER of the User
func CreateCypherRelationshipOperationTo(toNodeIndexValue string, unique *neo4j.Unique, relName string) *neo4j.Cypher {
	query := fmt.Sprintf("START you = node:%v(%v='%v'), me = node:%v(%v='%v') CREATE UNIQUE you-[new_rel:%v]-> me RETURN new_rel,you",
		unique.IndexName, unique.Key, unique.Value, unique.IndexName, unique.Key, toNodeIndexValue, relName)
	cypher := &neo4j.Cypher{
		Query: map[string]string{
			"query": query,
		},
		Payload: map[string]interface{}{},
	}
	return cypher
}
