// Package contact — знакомство и круг общения molva: инвайт-ссылки,
// accept/reject, локальные алиасы, блокировка и presence контактов.
// Авторитетен только NodeID; глобальных имён нет, любое самоназвание
// пира — лишь подсказка.
package contact

import (
	"errors"
	"fmt"
	"hash/crc32"
	"math/big"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/udisondev/molva/peer"
)

const (
	inviteScheme = "molva"
	inviteHost   = "add"
	// MaxAliasLen — потолок алиаса в рунах (и предлагаемого, и локального).
	MaxAliasLen = 64
)

// Ошибки разбора инвайта (недоверенный ввод пользователя/QR).
var (
	ErrBadInvite    = errors.New("contact: не похоже на инвайт molva")
	ErrBadChecksum  = errors.New("contact: инвайт повреждён (чексумма)")
	ErrBadInviteLen = errors.New("contact: инвайт повреждён (длина)")
)

// base58Alphabet — алфавит Bitcoin: без 0/O/I/l, удобно диктовать.
const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// EncodeInvite собирает инвайт-ссылку: molva://add/<base58(id||crc32)>?a=<алиас>.
// CRC32 ловит опечатку до похода в сеть; алиас — предлагаемый, локально
// редактируем.
func EncodeInvite(id peer.ID, alias string) string {
	payload := make([]byte, 0, peer.IDLen+4)
	payload = append(payload, id[:]...)
	crc := crc32.ChecksumIEEE(id[:])
	payload = append(payload, byte(crc>>24), byte(crc>>16), byte(crc>>8), byte(crc))
	s := inviteScheme + "://" + inviteHost + "/" + base58Encode(payload)
	if alias != "" {
		s += "?a=" + url.QueryEscape(clampAlias(alias))
	}
	return s
}

// ParseInvite разбирает инвайт-ссылку. Возвращает NodeID и предлагаемый
// алиас (может быть пустым). Произвольный мусор не паникует.
func ParseInvite(s string) (peer.ID, string, error) {
	u, err := url.Parse(strings.TrimSpace(s))
	if err != nil {
		return peer.ID{}, "", fmt.Errorf("%w: %w", ErrBadInvite, err)
	}
	if u.Scheme != inviteScheme || u.Host != inviteHost {
		return peer.ID{}, "", ErrBadInvite
	}
	code := strings.TrimPrefix(u.Path, "/")
	if code == "" || strings.Contains(code, "/") {
		return peer.ID{}, "", ErrBadInvite
	}
	payload, err := base58Decode(code)
	if err != nil {
		return peer.ID{}, "", err
	}
	if len(payload) != peer.IDLen+4 {
		return peer.ID{}, "", ErrBadInviteLen
	}
	var id peer.ID
	copy(id[:], payload[:peer.IDLen])
	want := crc32.ChecksumIEEE(id[:])
	got := uint32(payload[32])<<24 | uint32(payload[33])<<16 | uint32(payload[34])<<8 | uint32(payload[35])
	if want != got {
		return peer.ID{}, "", ErrBadChecksum
	}
	return id, clampAlias(u.Query().Get("a")), nil
}

// clampAlias чистит алиас: валидный UTF-8, без управляющих, не длиннее
// MaxAliasLen рун.
func clampAlias(a string) string {
	if !utf8.ValidString(a) {
		return ""
	}
	var b strings.Builder
	n := 0
	for _, r := range a {
		if r < 0x20 || r == 0x7f {
			continue
		}
		b.WriteRune(r)
		n++
		if n == MaxAliasLen {
			break
		}
	}
	return strings.TrimSpace(b.String())
}

// base58Encode — классический base58: ведущие нули как '1'.
func base58Encode(b []byte) string {
	zeros := 0
	for zeros < len(b) && b[zeros] == 0 {
		zeros++
	}
	x := new(big.Int).SetBytes(b)
	base := big.NewInt(58)
	mod := new(big.Int)
	var out []byte
	for x.Sign() > 0 {
		x.DivMod(x, base, mod)
		out = append(out, base58Alphabet[mod.Int64()])
	}
	for range zeros {
		out = append(out, base58Alphabet[0])
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return string(out)
}

// base58Decode — обратное преобразование; чужие символы — ошибка.
func base58Decode(s string) ([]byte, error) {
	zeros := 0
	for zeros < len(s) && s[zeros] == '1' {
		zeros++
	}
	x := new(big.Int)
	base := big.NewInt(58)
	for _, c := range []byte(s) {
		idx := strings.IndexByte(base58Alphabet, c)
		if idx < 0 {
			return nil, ErrBadInvite
		}
		x.Mul(x, base)
		x.Add(x, big.NewInt(int64(idx)))
	}
	body := x.Bytes()
	out := make([]byte, zeros+len(body))
	copy(out[zeros:], body)
	return out, nil
}
