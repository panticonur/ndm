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
	messages *list.List // FIFO готовых сообщений
	waiters  *list.List // FIFO ожидающих получателей (хранит chan string)
}

type broker struct {
	mu     sync.Mutex
	queues map[string]*queue // реестр очередей
}

func (b *broker) getQueue(name string) *queue {
	q := b.queues[name]
	if q == nil {
		q = &queue{messages: list.New(), waiters: list.New()}
		b.queues[name] = q
	}
	return q
}

// Удаляет очередь из реестра, если в ней нет ни сообщений, ни ожидающих.
// Вызывается только под мьютексом.
func (b *broker) dropIfEmpty(name string, q *queue) {
	if q.messages.Len() == 0 && q.waiters.Len() == 0 {
		delete(b.queues, name)
	}
}

func (b *broker) put(name, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.getQueue(name)
	// Отдаём сообщение ожидающему из головы очереди - тому, кто был первым.
	if e := q.waiters.Front(); e != nil {
		q.waiters.Remove(e)
		e.Value.(chan string) <- msg
		b.dropIfEmpty(name, q) // Если отдали последнему, то очередь опустела.
		return
	}
	q.messages.PushBack(msg)
}

func (b *broker) get(name string, timeout time.Duration) (string, bool) {
	b.mu.Lock()
	q := b.getQueue(name)
	// Хотя бы один из списков messages и waiters всегда пуст.
	// При существующем ждущем put отдаёт сразу ему, копиться сообщениям не даёт.
	// Поэтому сперва берём готовое сообщение, а если его нет, то ждём.
	if e := q.messages.Front(); e != nil { // сообщение уже готово
		msg := q.messages.Remove(e).(string)
		b.dropIfEmpty(name, q)
		b.mu.Unlock()
		return msg, true
	}
	if timeout == 0 {
		b.dropIfEmpty(name, q)
		b.mu.Unlock()
		return "", false
	}
	// put доставляет сообщение получателю, посылая его в ch ПОД мьютексом.
	// В это время get может уйти в ветку time.After и сам будет ожидать мьютекс,
	// который держит put. При небуферизованном канале вышла бы взаимная блокировка.
	// Буферизованный канал даёт put возможность положить сообщение и сразу отпустить мьютекс,
	// а проснувшийся по таймауту get заберёт его из буфера во вложенном select ниже.
	// Размер 1: у каждого ожидающего свой канал, и put шлёт в него максимум одно сообщение.
	ch := make(chan string, 1)
	we := q.waiters.PushBack(ch) // Добавляем в хвост очереди.
	b.mu.Unlock()

	// Ждём вне мьютекса: либо put положит сообщение в наш канал, либо истечёт таймаут.
	select {
	case msg := <-ch:
		return msg, true
	case <-time.After(timeout):
		// put мог отдать нам сообщение в зазоре между срабатыванием таймаута
		// и захватом мьютекса.
		b.mu.Lock()
		defer b.mu.Unlock()
		// Поэтому под локом проверяем канал, чтобы сообщение не потерялось.
		select {
		case msg := <-ch:
			return msg, true
		default: // Если сообщения нет, значит мы реально таймаутнули,
			q.waiters.Remove(we) // снимаем себя из очереди ожидающих,
			b.dropIfEmpty(name, q)
			return "", false // сообщения нет — таймаут; 404 поставит handle.
		}
	}
}

func (b *broker) handle(w http.ResponseWriter, r *http.Request) {
	// Имя очереди = путь без ведущего "/". Пустое имя это валидная очередь "".
	// TrimPrefix, а не Path[1:]: у absolute-form запроса без пути (GET http://host)
	// r.URL.Path == "", и Path[1:] паниковал бы, а TrimPrefix на "" вернёт "".
	name := strings.TrimPrefix(r.URL.Path, "/")
	switch r.Method {
	case http.MethodPut:
		q := r.URL.Query()
		// Has отличает «нет параметра» от «параметр пустой».
		// Get вернёт "" и при отсутствии v, и при пустом значении (?v=).
		// 400 нужен только когда v отсутствует, а ?v= валидное пустое сообщение.
		if !q.Has("v") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		b.put(name, q.Get("v"))
	case http.MethodGet:
		// нет/битый timeout → 0 → не ждём
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
		log.Fatal("usage: queue_broker <port>")
	}
	b := &broker{queues: map[string]*queue{}}
	log.Fatal(http.ListenAndServe(":"+os.Args[1], http.HandlerFunc(b.handle)))
}
