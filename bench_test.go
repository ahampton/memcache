package memcache

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func benchmarkSet(b *testing.B, item *Item) {
	cmd, c := newUnixServer(b)
	c.SetTimeout(time.Duration(-1))
	b.SetBytes(int64(len(item.Key) + len(item.Value)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Set(item); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	cmd.Process.Kill()
	cmd.Wait()
}

func benchmarkSetGet(b *testing.B, item *Item) {
	cmd, c := newUnixServer(b)
	c.SetTimeout(time.Duration(-1))
	key := item.Key
	b.SetBytes(int64(len(item.Key) + len(item.Value)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Set(item); err != nil {
			b.Fatal(err)
		}
		if _, err := c.Get(key); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	cmd.Process.Kill()
	cmd.Wait()
}

func benchmarkSetQuietly(b *testing.B, item *Item) {
	cmd, c := newUnixServer(b)
	c.SetTimeout(time.Duration(-1))
	b.SetBytes(int64(len(item.Key) + len(item.Value)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.SetQuietly(item); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	cmd.Process.Kill()
	cmd.Wait()
}

func benchmarkSetGetQuietly(b *testing.B, item *Item) {
	cmd, c := newUnixServer(b)
	c.SetTimeout(time.Duration(-1))
	key := item.Key
	b.SetBytes(int64(len(item.Key) + len(item.Value)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.SetQuietly(item); err != nil {
			b.Fatal(err)
		}
		if _, err := c.Get(key); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	cmd.Process.Kill()
	cmd.Wait()
}

func benchmarkSetMulti(b *testing.B, items []*Item) {
	cmd, c := newUnixServer(b)
	c.SetTimeout(time.Duration(-1))
	bytes := 0
	for _, item := range items {
		bytes += len(item.Key) + len(item.Value)
	}
	b.SetBytes(int64(bytes))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.SetMulti(items); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	cmd.Process.Kill()
	cmd.Wait()
}

func benchmarkSetGetMulti(b *testing.B, items []*Item) {
	cmd, c := newUnixServer(b)
	c.SetTimeout(time.Duration(-1))
	keys := make([]string, len(items))
	bytes := 0
	for i, item := range items {
		bytes += len(item.Key) + len(item.Value)
		keys[i] = item.Key
	}
	b.SetBytes(int64(bytes))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.SetMulti(items); err != nil {
			b.Fatal(err)
		}
		if _, err := c.GetMulti(keys); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	cmd.Process.Kill()
	cmd.Wait()
}

func benchmarkSetMultiQuietly(b *testing.B, items []*Item) {
	cmd, c := newUnixServer(b)
	c.SetTimeout(time.Duration(-1))
	bytes := 0
	for _, item := range items {
		bytes += len(item.Key) + len(item.Value)
	}
	b.SetBytes(int64(bytes))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.SetMultiQuietly(items); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	cmd.Process.Kill()
	cmd.Wait()
}

func benchmarkSetGetMultiQuietly(b *testing.B, items []*Item) {
	cmd, c := newUnixServer(b)
	c.SetTimeout(time.Duration(-1))
	keys := make([]string, len(items))
	bytes := 0
	for i, item := range items {
		bytes += len(item.Key) + len(item.Value)
		keys[i] = item.Key
	}
	b.SetBytes(int64(bytes))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.SetMultiQuietly(items); err != nil {
			b.Fatal(err)
		}
		if _, err := c.GetMulti(keys); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	cmd.Process.Kill()
	cmd.Wait()
}

func largeItem() *Item {
	key := strings.Repeat("f", 240)
	value := make([]byte, 1024)
	return &Item{Key: key, Value: value}
}

func smallItem() *Item {
	return &Item{Key: "foo", Value: []byte("bar")}
}

func generateSmallItems(count int) []*Item {
	items := make([]*Item, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("foo_%d", i)
		items[i] = &Item{Key: key, Value: []byte("bar")}
	}
	return items
}

func generateLargeItems(count int) []*Item {
	items := make([]*Item, count)
	for i := 0; i < count; i++ {
		suffix := fmt.Sprintf("_%d", i)
		key := strings.Repeat("f", 240 - len(suffix))
		value := make([]byte, 1024)
		items[i] = &Item{Key: key + suffix, Value: value}
	}
	return items
}

func BenchmarkSet(b *testing.B) {
	benchmarkSet(b, smallItem())
}

func BenchmarkSetLarge(b *testing.B) {
	benchmarkSet(b, largeItem())
}

func BenchmarkSetGet(b *testing.B) {
	benchmarkSetGet(b, smallItem())
}

func BenchmarkSetGetLarge(b *testing.B) {
	benchmarkSetGet(b, largeItem())
}

func BenchmarkSetQuietly(b *testing.B) {
	benchmarkSetQuietly(b, smallItem())
}

func BenchmarkSetQuietlyLarge(b *testing.B) {
	benchmarkSetQuietly(b, largeItem())
}

func BenchmarkSetGetQuietly(b *testing.B) {
	benchmarkSetGetQuietly(b, smallItem())
}

func BenchmarkSetGetQuietlyLarge(b *testing.B) {
	benchmarkSetGetQuietly(b, largeItem())
}

func BenchmarkSetMultiSingle(b *testing.B) {
	benchmarkSetMulti(b, generateSmallItems(1))
}

func BenchmarkSetMulti(b *testing.B) {
	benchmarkSetMulti(b, generateSmallItems(10))
}

func BenchmarkSetMultiSingleLarge(b *testing.B) {
	benchmarkSetMulti(b, generateLargeItems(1))
}

func BenchmarkSetMultiLarge(b *testing.B) {
	benchmarkSetMulti(b, generateLargeItems(10))
}

func BenchmarkSetGetMulti(b *testing.B) {
	benchmarkSetGetMulti(b, generateSmallItems(10))
}

func BenchmarkSetGetMultiLarge(b *testing.B) {
	benchmarkSetGetMulti(b, generateLargeItems(10))
}

func BenchmarkSetMultiQuietlySingle(b *testing.B) {
	benchmarkSetMultiQuietly(b, generateSmallItems(1))
}

func BenchmarkSetMultiQuietly(b *testing.B) {
	benchmarkSetMultiQuietly(b, generateSmallItems(10))
}

func BenchmarkSetMultiQuietlySingleLarge(b *testing.B) {
	benchmarkSetMultiQuietly(b, generateLargeItems(1))
}

func BenchmarkSetMultiQuietlyLarge(b *testing.B) {
	benchmarkSetMultiQuietly(b, generateLargeItems(10))
}

func BenchmarkSetGetMultiQuietly(b *testing.B) {
	benchmarkSetGetMultiQuietly(b, generateSmallItems(10))
}

func BenchmarkSetGetMultiQuietlyLarge(b *testing.B) {
	benchmarkSetGetMultiQuietly(b, generateLargeItems(10))
}

func benchmarkConcurrentSetGet(b *testing.B, item *Item, count int, opcount int) {
	mp := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(mp)
	runtime.GOMAXPROCS(count)
	cmd, c := newUnixServer(b)
	c.SetTimeout(time.Duration(-1))
	// Items are not thread safe
	items := make([]*Item, count)
	for ii := range items {
		items[ii] = &Item{Key: item.Key, Value: item.Value}
	}
	b.SetBytes(int64((len(item.Key) + len(item.Value)) * count * opcount))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(count)
		for j := 0; j < count; j++ {
			it := items[j]
			key := it.Key
			go func() {
				defer wg.Done()
				for k := 0; k < opcount; k++ {
					if err := c.Set(it); err != nil {
						b.Fatal(err)
					}
					if _, err := c.Get(key); err != nil {
						b.Fatal(err)
					}
				}
			}()
		}
		wg.Wait()
	}
	b.StopTimer()
	cmd.Process.Kill()
	cmd.Wait()
}

func BenchmarkGetCacheMiss(b *testing.B) {
	key := "not"
	cmd, c := newUnixServer(b)
	c.SetTimeout(time.Duration(-1))
	c.Delete(key)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Get(key); err != ErrCacheMiss {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	cmd.Process.Kill()
	cmd.Wait()
}

func BenchmarkConcurrentSetGetSmall10_100(b *testing.B) {
	benchmarkConcurrentSetGet(b, smallItem(), 10, 100)
}

func BenchmarkConcurrentSetGetLarge10_100(b *testing.B) {
	benchmarkConcurrentSetGet(b, largeItem(), 10, 100)
}

func BenchmarkConcurrentSetGetSmall20_100(b *testing.B) {
	benchmarkConcurrentSetGet(b, smallItem(), 20, 100)
}

func BenchmarkConcurrentSetGetLarge20_100(b *testing.B) {
	benchmarkConcurrentSetGet(b, largeItem(), 20, 100)
}
