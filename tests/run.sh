#!/usr/bin/env bash
# Функциональные тесты брокера очередей (main.go) — чёрный ящик через HTTP.
#
# Запуск:   bash tests/run.sh [PORT]      (PORT по умолчанию 8080)
# Требуется: go в PATH и curl.
# Скрипт сам собирает и запускает сервер, гоняет сценарии из задания
# и в конце гасит сервер. Код возврата 0 — все тесты прошли.

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
    echo "  ok   $desc"; PASS=$((PASS+1))
  else
    echo "  FAIL $desc — получили [$code '$body'], ждали [$wcode '$wbody']"; FAIL=$((FAIL+1))
  fi
}

# --- сборка и запуск сервера ---
BIN="$(mktemp -u).exe"
go build -o "$BIN" main.go || { echo "build failed"; exit 1; }
"$BIN" "$PORT" &
SRV_PID=$!

trap 'kill $SRV_PID 2>/dev/null; rm -f "$BIN"' EXIT
sleep 1 # Даём процессу время стартовать и проверяем, что он жив
kill -0 "$SRV_PID" 2>/dev/null  || { echo "run failed"; exit 1; }


echo "1) PUT кладёт, GET забирает по FIFO, пустая очередь -> 404"
expect "put pet=cat"           PUT "/pet?v=cat"        200 ""
expect "put pet=dog"           PUT "/pet?v=dog"        200 ""
expect "put pet=''"            PUT "/pet?v="           200 ""
expect "put role=manager"      PUT "/role?v=manager"   200 ""
expect "put role=executive"    PUT "/role?v=executive" 200 ""
expect "get pet -> cat"        GET "/pet"              200 "cat"
expect "get pet -> dog"        GET "/pet"              200 "dog"
expect "get pet -> ''"         GET "/pet"              200 ""
expect "get pet -> 404"        GET "/pet"              404 ""
expect "get pet -> 404"        GET "/pet"              404 ""
expect "get role -> manager"   GET "/role"             200 "manager"
expect "get role -> executive" GET "/role"             200 "executive"
expect "get role -> 404"       GET "/role"             404 ""

echo "2) PUT без параметра v -> 400"
expect "put без v -> 400"      PUT "/pet"              400 ""

echo "3) PUT без PATH"
expect "put без PATH"          PUT "?v=1"              200 ""
expect "put без PATH"          PUT "?v=2"              200 ""
expect "put пустой PATH"       PUT "/?v=3"             200 ""
expect "put пустой PATH"       PUT "/?v=4"             200 ""
expect "get без PATH"          GET ""                  200 "1"
expect "get без PATH"          GET "/"                 200 "2"
expect "get без PATH"          GET ""                  200 "3"
expect "get без PATH"          GET "/"                 200 "4"

echo "3) GET без timeout на пустой очереди -> сразу 404"
expect "get пусто, без timeout" GET "/none"            404 ""

echo "4) GET с timeout, сообщение не пришло -> 404 после ожидания"
t0=$SECONDS
expect "get timeout=1 -> 404"  GET "/wait?timeout=1"   404 ""
echo "     (ждали ~$((SECONDS - t0))s)"

echo "5) GET ждёт по timeout и получает пришедшее сообщение"
L=$(mktemp)
curl -s "$BASE/late?timeout=3" >"$L" &
PL=$!
sleep 1
curl -s -X PUT "$BASE/late?v=ping" >/dev/null
wait $PL
rl=$(cat "$L"); rm -f "$L"
if [[ "$rl" == "ping" ]]; then
  echo "  ok   дождался 'ping'"; PASS=$((PASS+1))
else
  echo "  FAIL получили '$rl', ждали 'ping'"; FAIL=$((FAIL+1))
fi

echo "6) Порядок ждущих: кто раньше запросил — тот раньше получил"
A=$(mktemp); B=$(mktemp)
curl -s "$BASE/ord?timeout=5" >"$A" &   # потребитель A (первый)
PA=$!
sleep 0.4
curl -s "$BASE/ord?timeout=5" >"$B" &   # потребитель B (второй)
PB=$!
sleep 0.4
curl -s -X PUT "$BASE/ord?v=first"  >/dev/null
sleep 0.2
curl -s -X PUT "$BASE/ord?v=second" >/dev/null
wait $PA $PB
ra=$(cat "$A"); rb=$(cat "$B"); rm -f "$A" "$B"
if [[ "$ra" == "first" && "$rb" == "second" ]]; then
  echo "  ok   A='first' B='second'"; PASS=$((PASS+1))
else
  echo "  FAIL A='$ra' B='$rb', ждали A='first' B='second'"; FAIL=$((FAIL+1))
fi

echo
echo "ИТОГО: passed=$PASS failed=$FAIL"

kill $SRV_PID
wait "$SRV_PID"
rm -f "$BIN"

[[ $FAIL -eq 0 ]]
