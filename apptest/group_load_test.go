package apptest

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"
)

// Нагрузочный срез групп: десять полных узлов, веерная рассылка от
// каждого участника каждому, всё доезжает ровно один раз.
func TestGroupLoadTenMembers(t *testing.T) {
	if testing.Short() {
		t.Skip("нагрузочный сценарий: пропуск в -short")
	}
	synctest.Test(t, func(t *testing.T) {
		const n = 10
		c := NewCluster(t, n)
		ctx := context.Background()

		idx := make([]int, n)
		for i := range idx {
			idx[i] = i
		}
		makeMesh(t, c, idx...)

		admin := c.Node(0)
		gid, err := admin.Core().Groups().Create(ctx, "полный зал")
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i < n; i++ {
			if err := admin.Core().Groups().Add(ctx, gid, c.Node(i).PeerID()); err != nil {
				t.Fatalf("Add(%d): %v", i, err)
			}
		}
		for i := 1; i < n; i++ {
			waitGroupKnown(t, c.Node(i), gid, uint64(n))
		}

		// Каждый говорит — все слышат.
		for i := range n {
			text := fmt.Sprintf("голос %d", i)
			if _, err := c.Node(i).Core().Groups().SendText(ctx, gid, text); err != nil {
				t.Fatalf("SendText(%d): %v", i, err)
			}
		}
		for i := range n {
			for j := range n {
				if i == j {
					continue
				}
				waitGroupMessage(t, c.Node(j), gid, fmt.Sprintf("голос %d", i))
			}
		}

		// Ровно по n сообщений в каждой истории, без дублей.
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		for j := range n {
			msgs, err := c.Node(j).Core().Groups().Messages(ctx, gid, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(msgs) != n {
				t.Fatalf("node-%d: %d сообщений, want %d", j, len(msgs), n)
			}
			seen := map[string]bool{}
			for _, m := range msgs {
				if seen[string(m.Body)] {
					t.Fatalf("node-%d: дубль %q", j, m.Body)
				}
				seen[string(m.Body)] = true
				if m.Sender == nil || !bytes.HasPrefix(m.Body, []byte("голос")) {
					t.Fatalf("node-%d: кривая запись %+v", j, m)
				}
			}
		}
	})
}
