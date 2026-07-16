// Consumer examples for redis-event-stream-module (issue #110), go-redis/v9.
//
// Subcommands map 1:1 to docs/consumer-patterns.md:
//
//	tail       live tail (pub/sub replacement)
//	work       durable work queue (consumer groups) + stuck-work recovery
//	reconcile  delimit capture gaps from the control stream's markers
//	discover   list destination streams via EVENTSTREAM.STREAMS
//
// Run against a server with the module loaded (default: expirations only):
//
//	go run . tail
//	REDIS_ADDR=10.0.0.5:6380 go run . work
//
// Binary-safe keys (SPEC.md section 6): "Consumers must read key with a
// bytes-typed client API; clients that eagerly decode replies as UTF-8 will
// mangle non-UTF-8 keys." go-redis returns field values as Go strings, which
// are byte-safe (a string can hold arbitrary bytes, unlike a validated
// unicode type), so key bytes round-trip exactly; recover them with []byte(v).
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/redis/go-redis/v9"
)

const (
	stream  = "events:expired" // the default-config destination stream
	group   = "workers"
	control = "events:#control" // the gap-marker control stream (SPEC.md section 9)
)

var ctx = context.Background()

func connect() *redis.Client {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	return redis.NewClient(&redis.Options{Addr: addr})
}

func consumerName() string {
	if c := os.Getenv("CONSUMER"); c != "" {
		return c
	}
	return "worker-1"
}

// show prints one mirrored entry. Values are strings holding raw bytes.
func show(id string, values map[string]interface{}) {
	event, _ := values["event"].(string)
	db, _ := values["db"].(string)
	keyStr, _ := values["key"].(string)
	keyBytes := []byte(keyStr) // the exact key bytes; no UTF-8 assumption
	fmt.Printf("  %s  event=%s db=%s key=%q (%d bytes)\n", id, event, db, keyStr, len(keyBytes))
}

func tail(r *redis.Client) error {
	last := "$" // only entries added after the first blocking call
	fmt.Printf("tailing %s (Ctrl-C to stop)\n", stream)
	for {
		res, err := r.XRead(ctx, &redis.XReadArgs{Streams: []string{stream, last}, Block: 0}).Result()
		if err != nil {
			return err
		}
		for _, s := range res {
			for _, msg := range s.Messages {
				show(msg.ID, msg.Values)
				last = msg.ID // resume from here, not $
			}
		}
	}
}

func work(r *redis.Client) error {
	consumer := consumerName()
	// MKSTREAM makes setup race-free against first capture; $ means "from now"
	// (use 0 to also process retained history).
	if err := r.XGroupCreateMkStream(ctx, stream, group, "$").Err(); err != nil &&
		!strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}

	// Startup: drain this consumer's own pending list (delivered-but-unacked,
	// e.g. a previous crash) by reading from id 0.
	pendingStart := "0"
	for {
		res, err := r.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: consumer,
			Streams: []string{stream, pendingStart}, Count: 100,
		}).Result()
		if err != nil && err != redis.Nil {
			return err
		}
		if len(res) == 0 || len(res[0].Messages) == 0 {
			break
		}
		for _, msg := range res[0].Messages {
			processAndAck(r, msg)
			pendingStart = msg.ID
		}
	}

	fmt.Printf("draining done; steady-state read as %s (Ctrl-C to stop)\n", consumer)
	sweeps := 0
	for {
		// > = entries never delivered to any consumer in this group.
		res, err := r.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: consumer,
			Streams: []string{stream, ">"}, Count: 100, Block: 5000,
		}).Result()
		if err != nil && err != redis.Nil {
			return err
		}
		for _, s := range res {
			for _, msg := range s.Messages {
				processAndAck(r, msg)
			}
		}
		if sweeps++; sweeps%4 == 0 {
			if err := reclaim(r, consumer); err != nil {
				return err
			}
		}
	}
}

func processAndAck(r *redis.Client, msg redis.XMessage) {
	show(msg.ID, msg.Values)
	// ... do the durable work here ...
	// Ack only after the work is durably done; a crash before this redelivers,
	// so processing must be idempotent (natural key: stream + entry ID).
	r.XAck(ctx, stream, group, msg.ID)
}

// reclaim reassigns entries idle > 60s from dead workers, dropping trimmed
// (nil-field) ones — treat those as lost, not work (SPEC.md section 9).
func reclaim(r *redis.Client, consumer string) error {
	msgs, _, err := r.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: stream, Group: group, Consumer: consumer,
		MinIdle: 60000, Start: "0-0", Count: 100,
	}).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	for _, msg := range msgs {
		if len(msg.Values) == 0 {
			continue // trimmed before we read it; XAUTOCLAIM already dropped it
		}
		processAndAck(r, msg)
	}
	return nil
}

// reconcile pairs open markers (disabled/unloading) with the next close
// (enabled/loaded) to print bounded capture-gap windows. Marker IDs are ms
// timestamps, usable directly as XRANGE bounds (see docs/loss-windows.md).
func reconcile(r *redis.Client) error {
	msgs, err := r.XRange(ctx, control, "-", "+").Result()
	if err != nil {
		return err
	}
	if len(msgs) == 0 {
		fmt.Println("no control stream yet (module never wrote a marker)")
		return nil
	}
	fmt.Printf("markers on %s:\n", control)
	var openID, openAction string
	for _, msg := range msgs {
		action, _ := msg.Values["action"].(string)
		version, _ := msg.Values["module-version"].(string)
		fmt.Printf("  %s  action=%s module-version=%s\n", msg.ID, action, version)
		switch action {
		case "disabled", "unloading":
			openID, openAction = msg.ID, action
		case "enabled", "loaded":
			if openID != "" {
				fmt.Printf("    -> gap window [%s .. %s] (%s -> %s); reconcile this range\n",
					openID, msg.ID, openAction, action)
				openID = ""
			}
		}
	}
	if openID != "" {
		fmt.Printf("    -> open gap since %s (%s); capture still down or crashed "+
			"(no closing marker)\n", openID, openAction)
	}
	return nil
}

// discover lists destination streams, skipping the module's own events:#* namespace.
func discover(r *redis.Client) error {
	names, err := r.Do(ctx, "EVENTSTREAM.STREAMS").StringSlice()
	if err != nil {
		return err
	}
	for _, name := range names {
		if strings.HasPrefix(name, "events:#") {
			continue // control/firehose streams are not event data
		}
		n, _ := r.XLen(ctx, name).Result()
		fmt.Printf("  %s  xlen=%d\n", name, n)
	}
	return nil
}

func main() {
	cmds := map[string]func(*redis.Client) error{
		"tail": tail, "work": work, "reconcile": reconcile, "discover": discover,
	}
	if len(os.Args) < 2 || cmds[os.Args[1]] == nil {
		fmt.Fprintln(os.Stderr, "usage: consumer {tail|work|reconcile|discover}")
		os.Exit(2)
	}
	if err := cmds[os.Args[1]](connect()); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
