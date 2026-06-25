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
	// Указатели, а не значения: list.List — самоссылающаяся структура (кольцо вокруг
	// сторожевого root), копировать её по значению нельзя — копия рвёт внутренние
	// ссылки. Через указатель копируется он сам, список остаётся целым.
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

// dropIfEmpty удаляет очередь из реестра, если в ней не осталось ни сообщений,
// ни ждущих. Вызывать только под b.mu.
func (b *broker) dropIfEmpty(name string, q *queue) {
	if q.messages.Len() == 0 && q.waiters.Len() == 0 {
		delete(b.queues, name)
	}
}

func (b *broker) put(name, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.getQueue(name)
	// Отдаём голове очереди ждущих — тому, кто запросил раньше всех (get встаёт
	// в хвост через PushBack). Так получатели обслуживаются в порядке запросов.
	if e := q.waiters.Front(); e != nil { // есть ждущий — отдаём сразу ему
		q.waiters.Remove(e)
		e.Value.(chan string) <- msg
		b.dropIfEmpty(name, q) // отдали последнему ждущему — очередь опустела
		return
	}
	q.messages.PushBack(msg)
}

func (b *broker) get(name string, timeout time.Duration) (string, bool) {
	b.mu.Lock()
	q := b.getQueue(name)
	// Инвариант: хотя бы один из списков всегда пуст — при живом ждущем put отдаёт
	// сразу ему, копиться сообщениям не даёт. Поэтому сперва берём готовое сообщение,
	// и лишь если его нет — встаём ждать.
	if e := q.messages.Front(); e != nil { // сообщение уже готово
		msg := q.messages.Remove(e).(string)
		b.dropIfEmpty(name, q)
		b.mu.Unlock()
		return msg, true
	}
	if timeout == 0 { // ждать не просили
		b.dropIfEmpty(name, q)
		b.mu.Unlock()
		return "", false
	}
	ch := make(chan string, 1) // буфер 1: put доставляет под мьютексом и не должен блокироваться
	we := q.waiters.PushBack(ch)
	b.mu.Unlock()

	// Ждём вне мьютекса: либо put положит сообщение в наш канал, либо истечёт таймаут.
	select {
	case msg := <-ch:
		return msg, true
	case <-time.After(timeout):
		b.mu.Lock()
		defer b.mu.Unlock()
		// Вложенный select закрывает гонку: put мог отдать нам сообщение в зазоре
		// между срабатыванием таймаута и захватом мьютекса. Поэтому под локом ещё раз
		// неблокирующе (ветка default) проверяем канал — иначе это сообщение
		// потерялось бы. Пришло — забираем; нет — снимаем себя из ждущих и отдаём 404.
		select {
		case msg := <-ch:
			return msg, true
		default:
			q.waiters.Remove(we)
			b.dropIfEmpty(name, q)
			return "", false
		}
	}
}

func (b *broker) handle(w http.ResponseWriter, r *http.Request) {
	// Имя очереди = путь без ведущего "/". TrimPrefix (не [1:]) не паникует на пустом
	// пути (OPTIONS *); пустое имя — просто валидная очередь "".
	name := strings.TrimPrefix(r.URL.Path, "/")
	switch r.Method {
	case http.MethodPut:
		q := r.URL.Query()
		// Has, а не Get(...) == "": Get вернёт "" и при отсутствии v, и при пустом
		// значении (?v=). 400 нужен только когда v отсутствует, а ?v= — валидное
		// пустое сообщение. Has отличает «нет параметра» от «параметр пустой».
		if !q.Has("v") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		b.put(name, q.Get("v"))
	case http.MethodGet:
		// Ошибку Atoi глотаем намеренно: нет timeout или мусор → 0 → не ждём, сразу отвечаем.
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
