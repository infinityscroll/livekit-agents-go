// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"errors"
	"fmt"
	"sync"
)

// RemoteChatItem is an immutable snapshot of an item in a RemoteChatContext.
// Empty previous/next IDs denote the head/tail respectively.
type RemoteChatItem struct {
	Item           ChatItem
	PreviousItemID string
	NextItemID     string
}

type remoteChatNode struct {
	item ChatItem
	prev *remoteChatNode
	next *remoteChatNode
}

// RemoteChatContext maintains the ordered, ID-addressable history used by
// realtime providers. Insert is O(1), as are lookup and deletion. Returned
// values are defensive copies and may be safely retained by callers.
type RemoteChatContext struct {
	mu    sync.RWMutex
	head  *remoteChatNode
	tail  *remoteChatNode
	byID  map[string]*remoteChatNode
	count int
}

func NewRemoteChatContext() *RemoteChatContext {
	return &RemoteChatContext{byID: make(map[string]*remoteChatNode)}
}

func (c *RemoteChatContext) ensureMap() {
	if c.byID == nil {
		c.byID = make(map[string]*remoteChatNode)
	}
}

func (c *RemoteChatContext) Len() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	n := c.count
	c.mu.RUnlock()
	return n
}

// ToChatContext returns an independent ChatContext in remote ordering.
func (c *RemoteChatContext) ToChatContext() *ChatContext {
	if c == nil {
		return EmptyChatContext()
	}
	c.mu.RLock()
	items := make([]ChatItem, 0, c.count)
	for node := c.head; node != nil; node = node.next {
		items = append(items, node.item.clone())
	}
	c.mu.RUnlock()
	return NewChatContext(items...)
}

// Get returns a snapshot of the node with itemID.
func (c *RemoteChatContext) Get(itemID string) (RemoteChatItem, bool) {
	if c == nil {
		return RemoteChatItem{}, false
	}
	c.mu.RLock()
	node, ok := c.byID[itemID]
	if !ok {
		c.mu.RUnlock()
		return RemoteChatItem{}, false
	}
	result := snapshotRemoteNode(node)
	c.mu.RUnlock()
	return result, true
}

func snapshotRemoteNode(node *remoteChatNode) RemoteChatItem {
	result := RemoteChatItem{Item: node.item.clone()}
	if node.prev != nil {
		result.PreviousItemID = node.prev.item.ItemID()
	}
	if node.next != nil {
		result.NextItemID = node.next.item.ItemID()
	}
	return result
}

// Insert adds item immediately after previousItemID. An empty previousItemID
// inserts at the head, matching the TypeScript SDK's undefined/root behavior.
func (c *RemoteChatContext) Insert(previousItemID string, item ChatItem) error {
	if c == nil {
		return errors.New("remote chat context is nil")
	}
	if item == nil || item.ItemID() == "" {
		return errors.New("chat item and id are required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureMap()
	itemID := item.ItemID()
	if _, exists := c.byID[itemID]; exists {
		return fmt.Errorf("item with ID %s already exists", itemID)
	}

	node := &remoteChatNode{item: item.clone()}
	if previousItemID == "" {
		node.next = c.head
		if c.head != nil {
			c.head.prev = node
		} else {
			c.tail = node
		}
		c.head = node
	} else {
		previous := c.byID[previousItemID]
		if previous == nil {
			return fmt.Errorf("previousItemID %s not found", previousItemID)
		}
		node.prev = previous
		node.next = previous.next
		previous.next = node
		if node.next != nil {
			node.next.prev = node
		} else {
			c.tail = node
		}
	}
	c.byID[itemID] = node
	c.count++
	return nil
}

// Delete removes itemID while preserving the order of its neighbors.
func (c *RemoteChatContext) Delete(itemID string) error {
	if c == nil {
		return errors.New("remote chat context is nil")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	node := c.byID[itemID]
	if node == nil {
		return fmt.Errorf("item with ID %s not found", itemID)
	}
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		c.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		c.tail = node.prev
	}
	delete(c.byID, itemID)
	c.count--
	node.prev, node.next = nil, nil
	return nil
}
