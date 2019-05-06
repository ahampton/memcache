/*
Copyright 2011 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package memcache provides a client for the memcached cache server.
package memcache

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const testServer = "localhost:11211"

func (c *Client) totalOpen() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, v := range c.freeconn {
		count += len(v)
	}
	return count
}

func newLocalhostServer(tb testing.TB) *Client {
	c, err := net.Dial("tcp", testServer)
	if err != nil {
		tb.Skipf("skipping test; no server running at %s", testServer)
		return nil
	}
	c.Write([]byte("flush_all\r\n"))
	c.Close()
	client, err := New(testServer)
	if err != nil {
		tb.Fatal(err)
	}
	return client
}

func newUnixServer(tb testing.TB) (*exec.Cmd, *Client) {
	sock := fmt.Sprintf("/tmp/test-gomemcache-%d.sock", os.Getpid())
	os.Remove(sock)
	cmd := exec.Command("memcached", "-s", sock)
	if err := cmd.Start(); err != nil {
		tb.Skip("skipping test; couldn't find memcached")
		return nil, nil
	}

	// Wait a bit for the socket to appear.
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		time.Sleep(time.Duration(25*i) * time.Millisecond)
	}
	c, err := New(sock)
	if err != nil {
		tb.Fatal(err)
	}
	return cmd, c
}

func TestLocalhost(t *testing.T) {
	testWithClient(t, newLocalhostServer(t))
}

// Run the memcached binary as a child process and connect to its unix socket.
func TestUnixSocket(t *testing.T) {
	cmd, c := newUnixServer(t)
	defer cmd.Wait()
	defer cmd.Process.Kill()
	testWithClient(t, c)
}

func testWithClient(t *testing.T, c *Client) {
	checkErr := func(err error, format string, args ...interface{}) {
		if err != nil {
			t.Fatalf(format, args...)
		}
	}

	mustSet := func(it *Item) {
		if err := c.Set(it); err != nil {
			t.Fatalf("failed to Set %#v: %v", *it, err)
		}
	}

	// Set
	foo := &Item{Key: "foo", Value: []byte("fooval"), Flags: 123}
	err := c.Set(foo)
	checkErr(err, "first set(foo): %v", err)
	err = c.Set(foo)
	checkErr(err, "second set(foo): %v", err)

	// Get
	it, err := c.Get("foo")
	checkErr(err, "get(foo): %v", err)
	if it.Key != "foo" {
		t.Errorf("get(foo) Key = %q, want foo", it.Key)
	}
	if string(it.Value) != "fooval" {
		t.Errorf("get(foo) Value = %q, want fooval", string(it.Value))
	}
	if it.Flags != 123 {
		t.Errorf("get(foo) Flags = %v, want 123", it.Flags)
	}

	// Get non-existant
	_, err = c.Get("not-exists")
	if err != ErrCacheMiss {
		t.Errorf("get(not-exists): expecting %v, got %v instead", ErrCacheMiss, err)
	}

	// Get and set a unicode key
	quxKey := "Hello_世界"
	qux := &Item{Key: quxKey, Value: []byte("hello world")}
	err = c.Set(qux)
	checkErr(err, "first set(Hello_世界): %v", err)
	it, err = c.Get(quxKey)
	checkErr(err, "get(Hello_世界): %v", err)
	if it.Key != quxKey {
		t.Errorf("get(Hello_世界) Key = %q, want Hello_世界", it.Key)
	}
	if string(it.Value) != "hello world" {
		t.Errorf("get(Hello_世界) Value = %q, want hello world", string(it.Value))
	}

	// Set malformed keys
	malFormed := &Item{Key: "foo bar", Value: []byte("foobarval")}
	err = c.Set(malFormed)
	if err != ErrMalformedKey {
		t.Errorf("set(foo bar) should return ErrMalformedKey instead of %v", err)
	}
	malFormed = &Item{Key: "foo" + string(0x7f), Value: []byte("foobarval")}
	err = c.Set(malFormed)
	if err != ErrMalformedKey {
		t.Errorf("set(foo<0x7f>) should return ErrMalformedKey instead of %v", err)
	}

	// SetQuietly
	quiet := &Item{Key: "quiet", Value: []byte("Shhh")}
	err = c.SetQuietly(quiet)
	checkErr(err, "setQuietly: %v", err)
	it, err = c.Get(quiet.Key)
	checkErr(err, "setQuietly: get: %v", err)
	if it.Key != quiet.Key {
		t.Errorf("setQuietly: get: Key = %q, want %s", it.Key, quiet.Key)
	}
	if string(it.Value) != string(quiet.Value) {
		t.Errorf("setQuietly: get: Value = %q, want %q", string(it.Value), string(quiet.Value))
	}

	// Add
	bar := &Item{Key: "bar", Value: []byte("barval")}
	err = c.Add(bar)
	checkErr(err, "first add(bar): %v", err)
	if err := c.Add(bar); err != ErrNotStored {
		t.Fatalf("second add(bar) want ErrNotStored, got %v", err)
	}

	// GetMulti
	m, err := c.GetMulti([]string{"foo", "bar"})
	checkErr(err, "GetMulti: %v", err)
	if g, e := len(m), 2; g != e {
		t.Errorf("GetMulti: got len(map) = %d, want = %d", g, e)
	}
	if _, ok := m["foo"]; !ok {
		t.Fatalf("GetMulti: didn't get key 'foo'")
	}
	if _, ok := m["bar"]; !ok {
		t.Fatalf("GetMulti: didn't get key 'bar'")
	}
	if g, e := string(m["foo"].Value), "fooval"; g != e {
		t.Errorf("GetMulti: foo: got %q, want %q", g, e)
	}
	if g, e := string(m["bar"].Value), "barval"; g != e {
		t.Errorf("GetMulti: bar: got %q, want %q", g, e)
	}

	// SetMulti
	baz1 := &Item{Key: "baz1", Value: []byte("baz1val")}
	baz2 := &Item{Key: "baz2", Value: []byte("baz2val"), Flags: 123}
	err = c.SetMulti([]*Item{baz1, baz2})
	checkErr(err, "first SetMulti: %v", err)
	err = c.SetMulti([]*Item{baz1, baz2})
	checkErr(err, "second SetMulti: %v", err)
	m, err = c.GetMulti([]string{baz1.Key, baz2.Key})
	checkErr(err, "SetMulti: %v", err)
	if g, e := len(m), 2; g != e {
		t.Errorf("SetMulti: got len(map) = %d, want = %d", g, e)
	}
	if _, ok := m[baz1.Key]; !ok {
		t.Fatalf("SetMulti: didn't get key '%s'", baz1.Key)
	}
	if _, ok := m[baz2.Key]; !ok {
		t.Fatalf("SetMulti: didn't get key '%s'", baz2.Key)
	}
	if g, e := string(m[baz1.Key].Value), string(baz1.Value); g != e {
		t.Errorf("SetMulti: got %q, want %q", g, e)
	}
	if g, e := string(m[baz2.Key].Value), string(baz2.Value); g != e {
		t.Errorf("SetMulti: got %q, want %q", g, e)
	}
	if m[baz1.Key].Flags != baz1.Flags {
		t.Errorf("SetMulti: Flags = %v, want %v", m[baz1.Key].Flags, baz1.Flags)
	}
	if m[baz2.Key].Flags != baz2.Flags {
		t.Errorf("SetMulti: Flags = %v, want %v", m[baz2.Key].Flags, baz2.Flags)
	}

	// SetMultiQuietly
	quiet1 := &Item{Key: "quiet1", Value: []byte("quiet1val")}
	quiet2 := &Item{Key: "quiet2", Value: []byte("quiet2val"), Flags: 123}
	err = c.SetMulti([]*Item{quiet1, quiet2})
	checkErr(err, "first SetMultiQuietly: %v", err)
	err = c.SetMulti([]*Item{quiet1, quiet2})
	checkErr(err, "second SetMultiQuietly: %v", err)
	m, err = c.GetMulti([]string{quiet1.Key, quiet2.Key})
	checkErr(err, "SetMultiQuietly: %v", err)
	if g, e := len(m), 2; g != e {
		t.Errorf("SetMultiQuietly: got len(map) = %d, want = %d", g, e)
	}
	if _, ok := m[quiet1.Key]; !ok {
		t.Fatalf("SetMultiQuietly: didn't get key '%s'", quiet1.Key)
	}
	if _, ok := m[quiet2.Key]; !ok {
		t.Fatalf("SetMultiQuietly: didn't get key '%s'", quiet2.Key)
	}
	if g, e := string(m[quiet1.Key].Value), string(quiet1.Value); g != e {
		t.Errorf("SetMultiQuietly: got %q, want %q", g, e)
	}
	if g, e := string(m[quiet2.Key].Value), string(quiet2.Value); g != e {
		t.Errorf("SetMultiQuietly: got %q, want %q", g, e)
	}
	if m[quiet1.Key].Flags != quiet1.Flags {
		t.Errorf("SetMultiQuietly: Flags = %v, want %v", m[quiet1.Key].Flags, quiet1.Flags)
	}
	if m[quiet2.Key].Flags != quiet2.Flags {
		t.Errorf("SetMultiQuietly: Flags = %v, want %v", m[quiet2.Key].Flags, quiet2.Flags)
	}

	// Delete
	key := "foo"
	item, err := c.Get(key)
	checkErr(err, "pre-Delete: %v", err)
	if item == nil {
		t.Error("pre-Delete want item, got nil")
	}
	err = c.Delete(key)
	checkErr(err, "Delete: %v", err)
	_, err = c.Get(key)
	if err != ErrCacheMiss {
		t.Error("post-Delete want ErrCacheMiss, got nil",)
	}

	// DeleteQuietly
	key = "quiet"
	item, err = c.Get(key)
	checkErr(err, "pre-DeleteQuietly: %v", err)
	if item == nil {
		t.Error("pre-DeleteQuietly want item, got nil",)
	}
	err = c.DeleteQuietly(key)
	checkErr(err, "DeleteQuietly: %v", err)
	_, err = c.Get(key)
	if err != ErrCacheMiss {
		t.Errorf("post-DeleteQuietly want ErrCacheMiss, got %v", err)
	}

	// DeleteMulti
	keys := []string{"baz1", "baz2"}
	items, err := c.GetMulti(keys)
	checkErr(err, "pre-DeleteMulti: %v", err)
	if len(items) != len(keys) {
		t.Errorf("pre-DeleteMulti want results, got %v", items)
	}
	err = c.DeleteMulti(keys)
	checkErr(err, "DeleteMulti: %v", err)
	items, err = c.GetMulti(keys)
	checkErr(err, "post-DeleteMulti: %v", err)
	if len(items) != 0 {
		t.Errorf("post-DeleteMulti want no results, got %v", items)
	}

	// DeleteMultiQuietly
	keys = []string{"quiet1", "quiet2"}
	items, err = c.GetMulti(keys)
	checkErr(err, "pre-DeleteMultiQuietly: %v", err)
	if len(items) != len(keys) {
		t.Errorf("pre-DeleteMultiQuietly want results, got %v", items)
	}
	err = c.DeleteMultiQuietly(keys)
	checkErr(err, "DeleteMultiQuietly: %v", err)
	items, err = c.GetMulti(keys)
	checkErr(err, "post-DeleteMultiQuietly: %v", err)
	if len(items) != 0 {
		t.Errorf("post-DeleteMultiQuietly want no results, got %v", items)
	}

	// Incr/Decr
	mustSet(&Item{Key: "num", Value: []byte("42")})
	n, err := c.Increment("num", 8)
	checkErr(err, "Increment num + 8: %v", err)
	if n != 50 {
		t.Fatalf("Increment num + 8: want=50, got=%d", n)
	}
	n, err = c.Decrement("num", 49)
	checkErr(err, "Decrement: %v", err)
	if n != 1 {
		t.Fatalf("Decrement 49: want=1, got=%d", n)
	}
	err = c.Delete("num")
	checkErr(err, "delete num: %v", err)
	n, err = c.Increment("num", 1)
	if err != ErrCacheMiss {
		t.Fatalf("increment post-delete: want ErrCacheMiss, got %v", err)
	}
	mustSet(&Item{Key: "num", Value: []byte("not-numeric")})
	n, err = c.Increment("num", 1)
	if err != ErrBadIncrDec {
		t.Fatalf("increment non-number: want %v, got %v", ErrBadIncrDec, err)
	}
	// Invalid key
	if err := c.Set(&Item{Key: strings.Repeat("f", 251), Value: []byte("bar")}); err != ErrMalformedKey {
		t.Errorf("expecting ErrMalformedKey when using key too long, got nil")
	}
	// Flush
	_, err = c.Get("bar")
	checkErr(err, "get(bar): %v", err)
	err = c.Flush(0)
	checkErr(err, "flush: %v", err)
	_, err = c.Get("bar")
	if err != ErrCacheMiss {
		t.Fatalf("post-flush: want ErrCacheMiss, got %v", err)
	}
}
