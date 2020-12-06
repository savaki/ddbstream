package ddbstream

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

const template = `
digraph {
%v;
%v;
}
`

type dag map[string]*dynamodbstreams.Shard

func (d dag) addShards(shards ...*dynamodbstreams.Shard) {
	for _, shard := range shards {
		id := aws.StringValue(shard.ShardId)
		d[id] = shard
	}
}

// equalNodes returns true if the shape of the dag
func (d dag) equalNodes(that dag) bool {
	if len(d) != len(that) {
		return false
	}
	for id := range d {
		if _, ok := that[id]; !ok {
			return false
		}
	}
	return true
}

func (d dag) Children(id string) (children []*dynamodbstreams.Shard) {
	for _, shard := range d {
		if parentID := aws.StringValue(shard.ParentShardId); parentID == id {
			children = append(children, shard)
		}
	}
	return children
}

func (d dag) FindAll(conditions ...condition) (shards []*dynamodbstreams.Shard) {
loop:
	for _, shard := range d {
		for _, condition := range conditions {
			if !condition(d, shard) {
				continue loop
			}
		}
		shards = append(shards, shard)
	}
	return shards
}

func (d dag) Walk(callback func(shard *dynamodbstreams.Shard) error, from ...string) error {
	var shards []*dynamodbstreams.Shard
	if len(from) > 0 {
		shards = d.FindAll(startingFrom(d, from...))
	} else {
		shards = d.Roots()
	}

	for _, shard := range shards {
		err := callback(shard)
		if err != nil {
			return err
		}

		err = d.walkID(aws.StringValue(shard.ShardId), callback)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d dag) walkID(id string, callback func(shard *dynamodbstreams.Shard) error) error {
	for _, shard := range d.Children(id) {
		err := callback(shard)
		if err != nil {
			return err
		}

		err = d.walkID(aws.StringValue(shard.ShardId), callback)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d dag) Roots() []*dynamodbstreams.Shard {
	return d.FindAll(roots)
}

func exportDAG(d dag, filename string) (err error) {
	lookup := map[string]string{}
	getNode := func(s *string) string {
		key := aws.StringValue(s)
		if s, ok := lookup[key]; ok {
			return s
		}
		value := fmt.Sprintf("S%d", len(lookup)+1)
		lookup[key] = value
		return value
	}

	var (
		edges []string
		nodes []string
	)

	callback := func(shard *dynamodbstreams.Shard) error {
		nodes = append(nodes, getNode(shard.ShardId))
		edges = append(edges, getNode(shard.ParentShardId)+" -> "+getNode(shard.ShardId))

		return nil
	}

	if err := d.Walk(callback); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return fmt.Errorf("unable to export dag: %w", err)
	}

	text := fmt.Sprintf(template, strings.Join(edges, ";\n"), strings.Join(nodes, ";\n"))

	if err := ioutil.WriteFile(filename, []byte(text), 0644); err != nil {
		return fmt.Errorf("unable to export dag: %w", err)
	}

	return nil
}

// condition provides a predicate function to see if
type condition func(dag, *dynamodbstreams.Shard) bool

func startingFrom(d dag, roots ...string) condition {
	ancestors := map[string]struct{}{}
	for _, id := range roots {
		if root, ok := d[id]; ok {
			if parentID := aws.StringValue(root.ParentShardId); parentID != "" {
				collectAncestors(d, parentID, ancestors)
			}
		}
	}

	return func(d dag, shard *dynamodbstreams.Shard) bool {
		got := aws.StringValue(shard.ShardId)
		if containsString(roots, got) {
			return true
		}

		if _, ok := ancestors[got]; ok {
			return false
		}

		parentID := aws.StringValue(shard.ParentShardId)
		_, hasAncestor := ancestors[parentID]
		if parentID == "" || d[parentID] == nil || hasAncestor {
			return true
		}

		return false
	}
}

func collectAncestors(d dag, id string, ancestors map[string]struct{}) {
	ancestors[id] = struct{}{}
	if shard, ok := d[id]; ok {
		if parentID := aws.StringValue(shard.ParentShardId); parentID != "" {
			ancestors[parentID] = struct{}{}
			collectAncestors(d, parentID, ancestors)
		}
	}
}

func roots(d dag, s *dynamodbstreams.Shard) bool {
	if parentID := aws.StringValue(s.ParentShardId); parentID == "" || d[parentID] == nil {
		return true
	}

	return false
}
