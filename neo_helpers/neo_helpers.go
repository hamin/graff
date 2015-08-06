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
// Neo4J Helpers that probably need to be extracted
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

func CreateCypherLabelOperation(unique *neo4j.Unique, label string) *neo4j.Cypher {
	query := fmt.Sprintf("START n = node:%v(%v='%v') set n %v return n", unique.IndexName, unique.Key, unique.Value, label)
	log.Info("CreateCypherLabelOperation error: %v", query)
	cypher := &neo4j.Cypher{
		Query: map[string]string{
			"query": query,
		},
		Payload: map[string]interface{}{},
	}
	return cypher
}

func CreateCypherRelationshipOperationFrom(fromNodeIndexValue string, unique *neo4j.Unique, relName string) *neo4j.Cypher {
	query := fmt.Sprintf("START me = node:%v(%v='%v'), you = node:%v(%v='%v') CREATE me-[new_rel:%v]-> you RETURN new_rel",
		unique.IndexName, unique.Key, fromNodeIndexValue, unique.IndexName, unique.Key, unique.Value, relName)
	log.Info("CreateCypherRelationshipOperationFrom error: %v", query)
	cypher := &neo4j.Cypher{
		Query: map[string]string{
			"query": query,
		},
		Payload: map[string]interface{}{},
	}
	return cypher
}

func CreateCypherRelationshipOperationTo(toNodeIndexValue string, unique *neo4j.Unique, relName string) *neo4j.Cypher {
	query := fmt.Sprintf("START you = node:%v(%v='%v'), me = node:%v(%v='%v') CREATE you-[new_rel:%v]-> me RETURN new_rel",
		unique.IndexName, unique.Key, unique.Value, unique.IndexName, unique.Key, toNodeIndexValue, relName)
	log.Info("CreateCypherRelationshipOperationTo error: %v", query)
	cypher := &neo4j.Cypher{
		Query: map[string]string{
			"query": query,
		},
		Payload: map[string]interface{}{},
	}
	return cypher
}

// AddLabelOperation - Neo4J Label Operation
func AddLabelOperation(batchOperations *[]*neo4j.ManuelBatchRequest, nodeIdx int, label string) {
	manuelLabel := &neo4j.ManuelBatchRequest{}
	manuelLabel.To = fmt.Sprintf("{%v}/labels", nodeIdx)
	manuelLabel.StringBody = label
	*batchOperations = append(*batchOperations, manuelLabel)
	if len(*batchOperations) < 1 {
		log.Error("The size of label batch operation is less than 1")
	}
}

// AddRelationshipOperation - This is probably wrong and needs work!
func AddRelationshipOperation(batchOperations *[]*neo4j.ManuelBatchRequest, startNodeIdx int, endNodeIdx int, startNodeIdxExist bool, endNodeIdxExist bool, relationshipType string) {
	manuelRelationship := &neo4j.ManuelBatchRequest{}

	body := make(map[string]interface{})
	body["type"] = relationshipType
	if startNodeIdxExist {
		manuelRelationship.To = fmt.Sprintf("/node/%v/relationships", startNodeIdx)
	} else {
		manuelRelationship.To = fmt.Sprintf("{%v}/relationships", startNodeIdx)
	}

	if endNodeIdxExist {
		body["to"] = fmt.Sprintf("%v", endNodeIdx)
	} else {
		body["to"] = fmt.Sprintf("{%v}", endNodeIdx)
	}

	manuelRelationship.Body = body

	*batchOperations = append(*batchOperations, manuelRelationship)
	if len(*batchOperations) < 1 {
		log.Error("The size of relationship batch operation is less than 1")
	}
}
