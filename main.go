package main

import (
	"container/list"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type queue struct {
	messages *list.List // FIFO готовых сообщений (string)
	waiters  *list.List // FIFO ожидающих получателей (chan string, буфер 1)
}

type broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

func (b *broker) getQueue(name string) *queue {
	q := b.queues[name]
	if q == nil {
		q = &queue{messages: list.New(), waiters: list.New()}
		b.queues[name] = q
	}
	return q
}

func (b *broker) put(name, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.getQueue(name)
	if e := q.waiters.Front(); e != nil { // есть ждущий — отдаём сразу ему
		q.waiters.Remove(e)
		e.Value.(chan string) <- msg
		return
	}
	q.messages.PushBack(msg)
}

func (b *broker) get(name string, timeout time.Duration) (string, bool) {
	b.mu.Lock()
	q := b.getQueue(name)
	if e := q.messages.Front(); e != nil { // сообщение уже готово
		q.messages.Remove(e)
		b.mu.Unlock()
		return e.Value.(string), true
	}
	if timeout == 0 { // ждать не просили
		b.mu.Unlock()
		return "", false
	}
	ch := make(chan string, 1)
	we := q.waiters.PushBack(ch)
	b.mu.Unlock()

	select {
	case msg := <-ch:
		return msg, true
	case <-time.After(timeout):
		b.mu.Lock()
		defer b.mu.Unlock()
		select {
		case msg := <-ch: // put успел доставить между таймаутом и локом
			return msg, true
		default:
			q.waiters.Remove(we)
			return "", false
		}
	}
}

func (b *broker) handle(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	switch r.Method {
	case http.MethodPut:
		q := r.URL.Query()
		if !q.Has("v") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		b.put(name, q.Get("v"))
	case http.MethodGet:
		timeout, _ := strconv.Atoi(r.URL.Query().Get("timeout"))
		if msg, ok := b.get(name, time.Duration(timeout)*time.Second); ok {
			w.Write([]byte(msg))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: queuebroker <port>")
	}
	b := &broker{queues: map[string]*queue{}}
	log.Fatal(http.ListenAndServe(":"+os.Args[1], http.HandlerFunc(b.handle)))
}
