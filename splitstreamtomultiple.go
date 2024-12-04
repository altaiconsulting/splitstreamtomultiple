// Copyright (c) 2024 ALTAI Consulting, Inc and Aleksey Gershgorin. All rights reserved.
// Use of this source code is governed by MIT license that can be found in the LICENSE file.

// Code below was written for the Medium Article "Using Golang Fan-Out Concurrency Pattern for Splitting Stream by Keys"
package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
)

// KeyValuePair represents a key-value pair with comparable key and arbitrary value
type KeyValuePair[K comparable, V any] struct {
	Key   K
	Value V
}

// IdentifiableChan represents channels identifiable by ID
type IdentifiableChan[T any] struct {
	ID   int
	Chan chan T
}

// FanOut splits the stream represented by inChan into separate streams differentiated by key
func FanOut[K comparable, V any](ctx context.Context, inChan <-chan KeyValuePair[K, V]) <-chan IdentifiableChan[KeyValuePair[K, V]] {
	fanOutMap := make(map[K]IdentifiableChan[KeyValuePair[K, V]])
	fanOutCh := make(chan IdentifiableChan[KeyValuePair[K, V]])
	go func() {
		chanID := 1
		for cont := true; cont; {
			select {
			case kvp, okCh := <-inChan:
				if !okCh {
					cont = false
				} else {
					_, okMap := fanOutMap[kvp.Key]
					if !okMap {
						fanOutMap[kvp.Key] = IdentifiableChan[KeyValuePair[K, V]]{ID: chanID, Chan: make(chan KeyValuePair[K, V])}
						chanID++
						fanOutCh <- fanOutMap[kvp.Key]
					}
					fanOutMap[kvp.Key].Chan <- kvp
				}
			case <-ctx.Done():
				cont = false
			}
		}
		for _, ch := range fanOutMap {
			close(ch.Chan)
		}
		close(fanOutCh)
	}()
	return fanOutCh
}

func main() {
	ctx := context.Background()
	inChan := make(chan KeyValuePair[string, int])
	// consumer of the split streams
	var wg sync.WaitGroup
	wg.Add(1)
	go func(ctx context.Context, inChan <-chan KeyValuePair[string, int]) {
		defer wg.Done()
		for ch := range FanOut(ctx, inChan) {
			wg.Add(1)
			go func(ctx context.Context, ch IdentifiableChan[KeyValuePair[string, int]]) {
				defer wg.Done()
				for cont := true; cont; {
					select {
					case <-ctx.Done():
						cont = false
					default:
						kvp, ok := <-ch.Chan
						if !ok {
							cont = false
						} else {
							fmt.Printf("printing from consumer %d: cusip %s, value %d\n", ch.ID, kvp.Key, kvp.Value)
						}
					}
				}
			}(ctx, ch)
		}
	}(ctx, inChan)
	// producer of the inChan stream
	cusips := []string{"cusip0001", "cusip0002", "cusip0003", "cusip0004", "cusip0005"}
	limit := 100
	for count := 0; count < limit; {
		randomIndex := rand.Intn(len(cusips))
		randomCusip := cusips[randomIndex]
		kvp := KeyValuePair[string, int]{randomCusip, count}
		inChan <- kvp
		count++
	}
	close(inChan)
	wg.Wait()
}
