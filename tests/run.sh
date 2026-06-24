#!/usr/bin/env bash
# Функциональные тесты брокера очередей (main.go).
#
# Запуск:   bash tests/run.sh [PORT]      (PORT по умолчанию 8080)
# Требуется: go в PATH и curl.
# Скрипт сам собирает и запускает сервер, гоняет сценарии из задания
# и в конце гасит сервер. Код возврата 0 — все тесты прошли.

# set -u — обращение к необъявленной переменной становится фатальной ошибкой,
# а не молчаливой пустой строкой. Ловит опечатки в именах (например $SRV_PD
# вместо $SRV_PID), которые иначе тихо превратились бы в пустой kill/rm и
# замаскировали ошибку.
set -u
PORT="${1:-8080}"
BASE="http://127.0.0.1:$PORT"
PASS=0
FAIL=0


# expect ОПИСАНИЕ МЕТОД URL ОЖИД_КОД ОЖИД_ТЕЛО
expect() {
  local desc=$1 method=$2 url=$3 wcode=$4 wbody=$5 out code body

  out=$(curl -s -X "$method" -w $'\n%{http_code}' "$BASE$url")
  code=${out##*$'\n'}
  body=${out%$'\n'*}
  
  if [[ "$code" == "$wcode" && "$body" == "$wbody" ]]; then
    echo "  OK   $desc"; PASS=$((PASS+1))
  else
    echo "  FAIL $desc — получили [$code '$body'], ждали [$wcode '$wbody']"; FAIL=$((FAIL+1))
  fi
}


# --- сборка и запуск сервера ---
BIN="$(mktemp -u).exe"
go build -o "$BIN" main.go || { echo "build failed"; exit 1; }
"$BIN" "$PORT" &
SRV_PID=$!
echo "Собираем и запускаем сервер на $PORT, бинарник $BIN, PID=$SRV_PID"

# Единый обработчик очистки. Повторный `trap ... EXIT` не добавляется, а ЗАМЕНЯЕТ
# предыдущий, поэтому весь cleanup держим в одном месте.
#
# cleanup ссылается на временные файлы тестов 6 и 7, а создаются они (mktemp)
# гораздо ниже. При досрочном выходе (например, сервер не поднялся на проверке
# kill -0) trap сработает, когда этих переменных ещё нет, и под set -u раскрытие
# "$TEST6_L" аварийно прервало бы сам обработчик. Поэтому объявляем их заранее
# пустыми: rm -f "" безвреден, а cleanup отрабатывает целиком при любом выходе.
TEST6_L=""; TEST7_A=""; TEST7_B=""
cleanup() { kill "$SRV_PID" 2>/dev/null; rm -f "$BIN" "$TEST6_L" "$TEST7_A" "$TEST7_B"; }
trap cleanup EXIT

sleep 1 # Даём процессу время стартовать и проверяем, что он жив
kill -0 "$SRV_PID" 2>/dev/null  || { echo "run failed"; exit 1; }


echo "1) PUT кладёт, GET забирает по FIFO, пустая очередь -> 404"
expect "PUT pet=cat"           PUT "/pet?v=cat"        200 ""
expect "PUT pet=dog"           PUT "/pet?v=dog"        200 ""
expect "PUT pet=''"            PUT "/pet?v="           200 ""
expect "PUT role=manager"      PUT "/role?v=manager"   200 ""
expect "PUT role=executive"    PUT "/role?v=executive" 200 ""
expect "GET pet -> cat"        GET "/pet"              200 "cat"
expect "GET pet -> dog"        GET "/pet"              200 "dog"
expect "GET pet -> ''"         GET "/pet"              200 ""
expect "GET pet -> 404"        GET "/pet"              404 ""
expect "GET pet -> 404"        GET "/pet"              404 ""
expect "GET role -> manager"   GET "/role"             200 "manager"
expect "GET role -> executive" GET "/role"             200 "executive"
expect "GET role -> 404"       GET "/role"             404 ""


echo "2) PUT без параметра v -> 400"
expect "put без v -> 400"      PUT "/pet"              400 ""


echo "3) PUT без PATH"
expect "PUT без PATH"          PUT "?v=1"              200 ""
expect "PUT пустой PATH"       PUT "/?v=2"             200 ""
expect "GET без PATH"          GET ""                  200 "1"
expect "GET пустой PATH"       GET "/"                 200 "2"


echo "4) GET без timeout на пустой очереди -> сразу 404"
expect "GET empty -> 404"      GET "/empty"            404 ""


echo "5) GET с timeout, сообщение не пришло -> 404 после ожидания"
t0=$SECONDS
expect "GET timeout=1 -> 404"  GET "/wait?timeout=1"   404 ""
echo "       (ждали ~$((SECONDS - t0))s)"

t0=$SECONDS
expect "GET timeout=3 -> 404"  GET "/wait?timeout=3"   404 ""
echo "       (ждали ~$((SECONDS - t0))s)"


echo "6) GET ждёт по timeout и получает пришедшее сообщение"
TEST6_L=$(mktemp)
curl -s "$BASE/late?timeout=3" >"$TEST6_L" &
PL=$!
sleep 1

curl -s -X PUT "$BASE/late?v=ping" >/dev/null
wait $PL

rl=$(cat "$TEST6_L")
if [[ "$rl" == "ping" ]]; then
  echo "  OK   дождался 'ping'"; PASS=$((PASS+1))
else
  echo "  FAIL получили '$rl', ждали 'ping'"; FAIL=$((FAIL+1))
fi


echo "7) Порядок ожидания: кто раньше запросил — тот раньше получил"
TEST7_A=$(mktemp); TEST7_B=$(mktemp)
curl -s "$BASE/ord?timeout=5" >"$TEST7_A" &   # потребитель A (первый)
PA=$!
sleep 0.4
curl -s "$BASE/ord?timeout=5" >"$TEST7_B" &   # потребитель B (второй)
PB=$!
sleep 0.4

curl -s -X PUT "$BASE/ord?v=first"  >/dev/null
sleep 0.2
curl -s -X PUT "$BASE/ord?v=second" >/dev/null
wait $PA $PB

ra=$(cat "$TEST7_A"); rb=$(cat "$TEST7_B")
if [[ "$ra" == "first" && "$rb" == "second" ]]; then
  echo "  OK   A='first' B='second'"; PASS=$((PASS+1))
else
  echo "  FAIL A='$ra' B='$rb', ждали A='first' B='second'"; FAIL=$((FAIL+1))
fi


echo; echo "ИТОГО: passed=$PASS failed=$FAIL"
[[ $FAIL -eq 0 ]]
