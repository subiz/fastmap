package fastmap

import (
	"container/heap"
	"strconv"
	"sync"
	"time"
)

type Fastmap struct {
	*sync.Mutex
	es  []*entry
	em  map[string]*entry
	eh  *entryHeap
	len int
}

func NewFastmap() *Fastmap {
	return &Fastmap{
		Mutex: &sync.Mutex{},
		es:    make([]*entry, 0),
		em:    make(map[string]*entry),
		eh:    &entryHeap{},
	}
}

// required key
// if stateless, append out alias
func (fm *Fastmap) Upsert(key string, value interface{}) string {
	if key == "" {
		return ""
	}
	fm.Lock()
	defer fm.Unlock()
	nows := time.Now().Unix()
	e := fm.em[key]

	if e != nil {
		if e.t != nows {
			e.t = nows
			heap.Fix(fm.eh, e.ih)
		}
		e.v = value
		return strconv.Itoa(e.i)
	}

	e = fm.eh.At(0)
	if e != nil && e.k == "" {
		e.k = key
		e.t = nows
		heap.Fix(fm.eh, e.ih)
		fm.em[key] = e
		e.v = value
		return strconv.Itoa(e.i)
	}

	e = &entry{k: key, t: nows, i: fm.len}
	fm.len++
	fm.es = append(fm.es, e)
	fm.eh.Push(e)
	fm.em[key] = e
	e.v = value
	return strconv.Itoa(e.i)
}

func (fm *Fastmap) Read(key, alias string) (interface{}, bool) {
	ki, err := strconv.Atoi(alias)
	if err != nil {
		return nil, false
	}
	if ki < 0 || ki >= fm.len {
		return nil, false
	}
	e := fm.es[ki]
	if e.k == "" || e.k != key {
		return nil, false
	}
	return e.v, true
}

func (fm *Fastmap) Readi(key string, ki int) (interface{}, bool) {
	if ki < 0 || ki >= fm.len {
		return nil, false
	}
	e := fm.es[ki]
	if e.k == "" || e.k != key {
		return nil, false
	}
	return e.v, true
}

func (fm *Fastmap) Delete(key string) {
	if key == "" {
		return
	}
	fm.Lock()
	defer fm.Unlock()
	e := fm.em[key]
	if e != nil {
		e.k = ""
		e.t = 0
		heap.Fix(fm.eh, e.ih)
	}
	delete(fm.em, key)
}

func (fm *Fastmap) List(f func(key string, value interface{}) bool) {
	n := fm.len
	var e *entry
	for i := 0; i < n; i++ {
		e = fm.es[i]
		if e == nil || e.k == "" {
			continue
		}
		if !f(e.k, e.v) {
			break
		}
	}
}

type entry struct {
	i  int
	ih int
	k  string
	v  interface{}
	t  int64
}

type entryHeap []*entry

func (h entryHeap) Len() int           { return len(h) }
func (h entryHeap) Less(i, j int) bool { return h[i].t < h[j].t }
func (h entryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].ih, h[j].ih = i, j }

func (h *entryHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*entry))
	ih := len(*h) - 1
	(*h)[ih].ih = ih
}

// don't use
func (h *entryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *entryHeap) At(i int) *entry {
	arr := *h
	if i >= 0 && i < len(arr) {
		return arr[i]
	}
	return nil
}
