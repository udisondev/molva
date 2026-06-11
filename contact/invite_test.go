package contact

import (
	"errors"
	"strings"
	"testing"

	"github.com/udisondev/molva/peer"
)

func TestInviteRoundTrip(t *testing.T) {
	ids := []peer.ID{
		{},                       // ведущие нули целиком
		{0, 0, 0, 1, 2, 3},       // ведущие нули частично
		{0xFF, 0xAB, 0x01, 0x99}, // обычный
	}
	for _, id := range ids {
		for _, alias := range []string{"", "Алиса", "имя с пробелами и 😀"} {
			inv := EncodeInvite(id, alias)
			gotID, gotAlias, err := ParseInvite(inv)
			if err != nil {
				t.Fatalf("ParseInvite(%q): %v", inv, err)
			}
			if gotID != id {
				t.Fatalf("id разошёлся: %x != %x", gotID[:4], id[:4])
			}
			if gotAlias != strings.TrimSpace(alias) {
				t.Fatalf("алиас разошёлся: %q != %q", gotAlias, alias)
			}
		}
	}
}

func TestInviteChecksumCatchesTypo(t *testing.T) {
	inv := EncodeInvite(peer.ID{1, 2, 3}, "")
	// Портим один символ кода (не префикс).
	body := strings.TrimPrefix(inv, "molva://add/")
	c := body[len(body)/2]
	repl := byte('2')
	if c == repl {
		repl = '3'
	}
	bad := "molva://add/" + body[:len(body)/2] + string(repl) + body[len(body)/2+1:]
	if _, _, err := ParseInvite(bad); err == nil {
		t.Fatal("опечатка обязана ловиться чексуммой")
	}
}

func TestInviteRejectsGarbage(t *testing.T) {
	cases := []string{
		"",
		"https://example.com/x",
		"molva://call/abc",
		"molva://add/",
		"molva://add/0OIl", // символы вне алфавита
		"molva://add/abc/def",
	}
	for _, s := range cases {
		if _, _, err := ParseInvite(s); err == nil {
			t.Fatalf("мусор принят: %q", s)
		}
	}
}

func TestInviteSelfErrors(t *testing.T) {
	if !errors.Is(func() error { _, _, err := ParseInvite("molva://add/111"); return err }(), ErrBadInviteLen) {
		t.Fatal("короткий код обязан давать ErrBadInviteLen")
	}
}

// FuzzParseInvite: произвольный ввод не паникует; успех — round-trip.
func FuzzParseInvite(f *testing.F) {
	f.Add("")
	f.Add(EncodeInvite(peer.ID{1, 2, 3}, "Алиса"))
	f.Add(EncodeInvite(peer.ID{}, ""))
	f.Add("molva://add/abc?a=%41%42")
	f.Add("molva://add/" + strings.Repeat("1", 100))

	f.Fuzz(func(t *testing.T, s string) {
		id, alias, err := ParseInvite(s)
		if err != nil {
			return
		}
		inv := EncodeInvite(id, alias)
		id2, alias2, err := ParseInvite(inv)
		if err != nil {
			t.Fatalf("re-parse: %v", err)
		}
		if id2 != id || alias2 != alias {
			t.Fatal("round-trip разошёлся")
		}
	})
}
