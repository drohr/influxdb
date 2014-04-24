package engine

import (
	"protocol"
	"sort"
)

// TODO: add some documentation

// A trie structure to map groups to aggregates, each level of the
// group corresponds to a field in the group by clause

// values at each level are sorted like this => nil, boolean, int64, float64, string

type Trie struct {
	size      int
	numLevels int
	numStates int
	topLevel  *Level

	// optimization for 0 columns trie
	zeroTrieNode *Node
}

type Level struct {
	nodes []*Node
}

type Node struct {
	isLeaf    bool
	value     *protocol.FieldValue // the value of the group by column corresponding to this level
	states    []interface{}        // the aggregator state
	nextLevel *Level               // the slice of the next level
}

func NewTrie(numLevels, numStates int) *Trie {
	trie := &Trie{0, numLevels, numStates, &Level{}, nil}
	if numLevels == 0 {
		trie.zeroTrieNode = &Node{true, nil, make([]interface{}, numStates), nil}
	}
	return trie
}

func (self *Trie) GetSize() int {
	return self.size
}

func (self *Trie) Traverse(f func([]*protocol.FieldValue, *Node) error) error {
	if self.numLevels == 0 {
		return f(nil, self.zeroTrieNode)
	}

	return self.topLevel.traverse(nil, f)
}

func (self *Trie) GetNode(values []*protocol.FieldValue) *Node {
	if len(values) != self.numLevels {
		panic("number of levels doesn't match values")
	}

	if self.numLevels == 0 {
		return self.zeroTrieNode
	}

	level := self.topLevel
	var node *Node
	var created bool
	for idx, v := range values {
		if self.numLevels-idx-1 > 0 {
			node, created = level.findOrCreateNode(v, 0)
		} else {
			node, created = level.findOrCreateNode(v, self.numStates)
		}
		level = node.nextLevel
	}

	if created {
		self.size++
	}
	node.isLeaf = true
	return node
}

func (self *Level) traverse(values []*protocol.FieldValue, f func([]*protocol.FieldValue, *Node) error) error {
	for _, node := range self.nodes {
		if node.isLeaf {
			err := f(append(values, node.value), node)
			if err != nil {
				return err
			}
			continue
		}
		return node.nextLevel.traverse(append(values, node.value), f)
	}
	return nil
}

func (self *Level) findOrCreateNode(value *protocol.FieldValue, numOfStates int) (*Node, bool) {
	idx := self.findNode(value)
	if idx == len(self.nodes) || !self.nodes[idx].value.Equals(value) {
		// add the new node
		node := self.createNode(value, numOfStates)
		self.nodes = append(self.nodes, node)
		// if idx is equal to the previous length, then leave it at the
		// end, otherwise, move it to that index.
		if idx != len(self.nodes)-1 {
			// shift all values to the right by one
			copy(self.nodes[idx+1:], self.nodes[idx:])
			self.nodes[idx] = node
		}
		return node, true
	}
	return self.nodes[idx], false
}

func (self *Level) createNode(value *protocol.FieldValue, numOfStates int) *Node {
	node := &Node{value: value}
	if numOfStates > 0 {
		node.states = make([]interface{}, numOfStates)
		return node
	}
	node.nextLevel = &Level{}
	return node
}

func (self *Level) findNode(value *protocol.FieldValue) int {
	return sort.Search(len(self.nodes), func(i int) bool {
		return self.nodes[i].value.GreaterOrEqual(value)
	})
}
