package group

import (
	"context"
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"slices"
	"time"

	"github.com/udisondev/molva/chat"
	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/outbox"
	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/senderkey"
	"github.com/udisondev/molva/store"
)

const (
	// sealedFlushTick — период повторных попыток отложенных рассылок
	// (ключи/членство ждут DR-сессий с адресатами).
	sealedFlushTick = 30 * time.Second
	// sealedBatch — рассылок за проход.
	sealedBatch = 128
	// labelAdmin — метка вывода группового админ-ключа из master-seed.
	labelAdmin = "molva/group/admin/v1"
)

// Manager — движок групп: членство, веерная рассылка через общий outbox,
// rekey на удаление. Служебные сообщения (welcome/update/ключи) едут по
// попарным DR-сессиям; до появления сессии лежат в персистентной
// sealed-очереди и ретраятся тикером.
type Manager struct {
	db        *store.DB
	ob        *outbox.Manager
	chats     *chat.Manager
	self      peer.ID
	adminPriv ed25519.PrivateKey
	rnd       io.Reader

	kick chan struct{}

	onMessage func(store.Message)
	ctr       counters
}

// NewManager выводит групповой админ-ключ из master-seed и регистрирует
// обработчики групповых конвертов.
func NewManager(db *store.DB, ob *outbox.Manager, chats *chat.Manager, seed [32]byte, self peer.ID) *Manager {
	b, err := hkdf.Key(sha256.New, seed[:], nil, labelAdmin, ed25519.SeedSize)
	if err != nil {
		panic("group: admin key: " + err.Error())
	}
	m := &Manager{
		db:        db,
		ob:        ob,
		chats:     chats,
		self:      self,
		adminPriv: ed25519.NewKeyFromSeed(b),
		rnd:       rand.Reader,
		kick:      make(chan struct{}, 1),
	}
	ob.Handle(envelope.TypeGroup, m.onGroupMessage)
	chats.RegisterSealed(envelope.TypeGroupWelcome, m.onWelcome)
	chats.RegisterSealed(envelope.TypeGroupUpdate, m.onUpdate)
	chats.RegisterSealed(envelope.TypeGroupKey, m.onKey)
	return m
}

// SetOnMessage — колбэк принятого группового сообщения (после коммита).
func (m *Manager) SetOnMessage(f func(store.Message)) { m.onMessage = f }

// AdminPub — наш групповой админ-ключ (для UI/отладки).
func (m *Manager) AdminPub() [32]byte {
	var pub [32]byte
	copy(pub[:], m.adminPriv.Public().(ed25519.PublicKey))
	return pub
}

// Run крутит отложенные sealed-рассылки до отмены ctx.
func (m *Manager) Run(ctx context.Context) error {
	ticker := time.NewTicker(sealedFlushTick)
	defer ticker.Stop()
	for {
		m.flushSealed(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.kick:
		case <-ticker.C:
		}
	}
}

// wake будит рассыльщик после постановки новых рассылок.
func (m *Manager) wake() {
	select {
	case m.kick <- struct{}{}:
	default:
	}
}

// flushSealed пробует отправить отложенные рассылки; без сессии — лежат
// дальше (рукопожатие уже запущено самим SendSealed).
func (m *Manager) flushSealed(ctx context.Context) {
	items, err := m.db.SealedOutboxList(ctx, sealedBatch)
	if err != nil {
		m.ctr.storeFailures.Add(1)
		return
	}
	for _, it := range items {
		if ctx.Err() != nil {
			return
		}
		_, err := m.chats.SendSealed(ctx, peer.ID(it.Peer), envelope.Type(it.EnvType), it.Payload)
		switch {
		case err == nil:
			err := m.db.Tx(ctx, func(tx *store.Tx) error { return tx.SealedOutboxDelete(it.ID) })
			if err != nil {
				m.ctr.storeFailures.Add(1)
			}
			m.ctr.sealedSent.Add(1)
		case errors.Is(err, chat.ErrNoSession):
			// Сессия в пути — следующий тик переиграет.
		default:
			m.ctr.sealedFailures.Add(1)
		}
	}
}

// Create создаёт группу: мы — админ и первый участник.
func (m *Manager) Create(ctx context.Context, name string) ([32]byte, error) {
	var gid [32]byte
	if _, err := io.ReadFull(m.rnd, gid[:]); err != nil {
		return gid, err
	}
	mem := Membership{
		GroupID:  gid,
		Version:  1,
		Name:     name,
		AdminPub: m.AdminPub(),
		Members:  []peer.ID{m.self},
	}
	mem.Sign(m.adminPriv)
	memBytes, err := EncodeMembership(mem)
	if err != nil {
		return gid, err
	}
	sender, err := senderkey.NewSender(m.rnd, 1)
	if err != nil {
		return gid, err
	}
	now := time.Now().UnixMilli()
	err = m.db.Tx(ctx, func(tx *store.Tx) error {
		if err := tx.GroupPut(&store.GroupRec{
			GroupID: gid, Name: name, AdminPub: mem.AdminPub, Version: 1,
			Membership: memBytes, CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			return err
		}
		if err := tx.GroupMembersSet(gid, mem.Members); err != nil {
			return err
		}
		return tx.SenderKeyPut(gid, m.self, sender.Marshal(), now)
	})
	if err != nil {
		return gid, err
	}
	return gid, nil
}

// Add вводит контакта в группу (только админ): новичку — welcome с
// текущими ключами, остальным — новая версия членства.
func (m *Manager) Add(ctx context.Context, gid [32]byte, member peer.ID) error {
	now := time.Now().UnixMilli()
	err := m.db.Tx(ctx, func(tx *store.Tx) error {
		rec, ok, err := tx.GroupGet(gid)
		if err != nil {
			return err
		}
		if !ok {
			return ErrUnknown
		}
		if rec.Left {
			return ErrLeft
		}
		if rec.AdminPub != m.AdminPub() {
			return ErrNotAdmin
		}
		old, err := DecodeMembership(rec.Membership)
		if err != nil {
			return err
		}
		if old.Has(member) {
			return nil // идемпотентно
		}
		if len(old.Members)+1 > MaxMembers {
			return ErrTooBig
		}

		next := Membership{
			GroupID:  gid,
			Version:  old.Version + 1,
			Name:     old.Name,
			AdminPub: old.AdminPub,
			Members:  append(slices.Clone(old.Members), member),
		}
		next.Sign(m.adminPriv)
		nextBytes, err := EncodeMembership(next)
		if err != nil {
			return err
		}
		rec.Version = next.Version
		rec.Membership = nextBytes
		rec.UpdatedAt = now
		if err := tx.GroupPut(&rec); err != nil {
			return err
		}
		if err := tx.GroupMembersSet(gid, next.Members); err != nil {
			return err
		}

		// Welcome новичку: членство + известные нам текущие ключи.
		keys, err := m.collectDists(tx, gid, next.Members, member)
		if err != nil {
			return err
		}
		welcome, err := EncodeWelcome(Welcome{Membership: next, Keys: keys})
		if err != nil {
			return err
		}
		if err := tx.SealedOutboxAdd(member, uint8(envelope.TypeGroupWelcome), welcome, now); err != nil {
			return err
		}
		// Update старым участникам.
		update, err := EncodeUpdate(next)
		if err != nil {
			return err
		}
		for _, p := range old.Members {
			if p == m.self {
				continue
			}
			if err := tx.SealedOutboxAdd(p, uint8(envelope.TypeGroupUpdate), update, now); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	m.wake()
	return nil
}

// Remove удаляет участника (только админ): новая версия членства и
// обязательный rekey оставшихся.
func (m *Manager) Remove(ctx context.Context, gid [32]byte, member peer.ID) error {
	now := time.Now().UnixMilli()
	err := m.db.Tx(ctx, func(tx *store.Tx) error {
		rec, ok, err := tx.GroupGet(gid)
		if err != nil {
			return err
		}
		if !ok {
			return ErrUnknown
		}
		if rec.AdminPub != m.AdminPub() {
			return ErrNotAdmin
		}
		old, err := DecodeMembership(rec.Membership)
		if err != nil {
			return err
		}
		if !old.Has(member) || member == m.self {
			return ErrNotMember
		}

		next := Membership{
			GroupID:  gid,
			Version:  old.Version + 1,
			Name:     old.Name,
			AdminPub: old.AdminPub,
		}
		for _, p := range old.Members {
			if p != member {
				next.Members = append(next.Members, p)
			}
		}
		next.Sign(m.adminPriv)
		nextBytes, err := EncodeMembership(next)
		if err != nil {
			return err
		}
		rec.Version = next.Version
		rec.Membership = nextBytes
		rec.UpdatedAt = now
		if err := tx.GroupPut(&rec); err != nil {
			return err
		}
		if err := tx.GroupMembersSet(gid, next.Members); err != nil {
			return err
		}
		if err := tx.SenderKeyDelete(gid, member); err != nil {
			return err
		}

		update, err := EncodeUpdate(next)
		if err != nil {
			return err
		}
		// Новая версия едет и удалённому: получив документ без себя, он
		// помечает группу покинутой (новых ключей он уже не увидит).
		for _, p := range old.Members {
			if p == m.self {
				continue
			}
			if err := tx.SealedOutboxAdd(p, uint8(envelope.TypeGroupUpdate), update, now); err != nil {
				return err
			}
		}
		// Обязательный rekey: бывший не должен читать новое.
		return m.rekeyTx(tx, gid, next.Members, now)
	})
	if err != nil {
		return err
	}
	m.wake()
	return nil
}

// rekeyTx генерит наш новый sender key и раскладывает его раздачу всем
// членам (кроме себя) в sealed-очередь — в той же транзакции.
func (m *Manager) rekeyTx(tx *store.Tx, gid [32]byte, members []peer.ID, nowMs int64) error {
	gen := uint32(1)
	if raw, ok, err := tx.SenderKeyGet(gid, m.self); err != nil {
		return err
	} else if ok {
		if old, err := senderkey.UnmarshalSender(raw); err == nil {
			gen = old.Generation() + 1
		}
	}
	sender, err := senderkey.NewSender(m.rnd, gen)
	if err != nil {
		return err
	}
	if err := tx.SenderKeyPut(gid, m.self, sender.Marshal(), nowMs); err != nil {
		return err
	}
	dist, err := EncodeKeyDist(gid, sender.Dist())
	if err != nil {
		return err
	}
	for _, p := range members {
		if p == m.self {
			continue
		}
		if err := tx.SealedOutboxAdd(p, uint8(envelope.TypeGroupKey), dist, nowMs); err != nil {
			return err
		}
	}
	m.ctr.rekeys.Add(1)
	return nil
}

// collectDists собирает текущие точки ключей участников для welcome.
func (m *Manager) collectDists(tx *store.Tx, gid [32]byte, members []peer.ID, exclude peer.ID) ([]MemberKey, error) {
	var out []MemberKey
	for _, p := range members {
		if p == exclude {
			continue
		}
		raw, ok, err := tx.SenderKeyGet(gid, p)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue // его ключ ещё не доехал — новичку его раздаст владелец
		}
		var d senderkey.Dist
		if p == m.self {
			s, err := senderkey.UnmarshalSender(raw)
			if err != nil {
				return nil, err
			}
			d = s.Dist()
		} else {
			r, err := senderkey.UnmarshalReceiver(raw)
			if err != nil {
				return nil, err
			}
			d = r.Dist()
		}
		out = append(out, MemberKey{Member: p, Key: d})
	}
	return out, nil
}

// SendText шифрует сообщение своим sender key'ем и рассылает веером всем
// участникам через общий надёжный outbox — одной транзакцией с историей.
func (m *Manager) SendText(ctx context.Context, gid [32]byte, text string) (envelope.MsgID, error) {
	if text == "" {
		return envelope.MsgID{}, chat.ErrEmptyText
	}
	if len(text) > chat.MaxTextLen {
		return envelope.MsgID{}, chat.ErrTooLong
	}
	mid, err := envelope.NewMsgID(m.rnd)
	if err != nil {
		return envelope.MsgID{}, err
	}
	now := time.Now().UnixMilli()
	var fanout []peer.ID
	err = m.db.Tx(ctx, func(tx *store.Tx) error {
		rec, ok, err := tx.GroupGet(gid)
		if err != nil {
			return err
		}
		if !ok {
			return ErrUnknown
		}
		if rec.Left {
			return ErrLeft
		}
		members, err := tx.GroupMembers(gid)
		if err != nil {
			return err
		}
		raw, ok, err := tx.SenderKeyGet(gid, m.self)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("group: нет собственного ключа группы")
		}
		sender, err := senderkey.UnmarshalSender(raw)
		if err != nil {
			return err
		}

		seq, err := tx.NextSeq("gseq:" + peer.ID(gid).String())
		if err != nil {
			return err
		}
		lam, err := tx.LamportNext()
		if err != nil {
			return err
		}

		n, ct, sig := sender.Encrypt(gid, []byte(text))
		msg := Msg{GroupID: gid, Generation: sender.Generation(), N: n, Ciphertext: ct}
		copy(msg.Signature[:], sig)
		payload, err := EncodeMsg(msg)
		if err != nil {
			return err
		}
		if err := tx.SenderKeyPut(gid, m.self, sender.Marshal(), now); err != nil {
			return err
		}
		if _, err := tx.InsertMessage(&store.Message{
			Peer: peer.ID(gid), MsgID: mid, Outgoing: true, FromSeq: seq, Lamport: lam,
			SentAt: now, Status: store.StatusSent, Body: []byte(text), Sender: m.self[:],
		}); err != nil {
			return err
		}
		env := envelope.Envelope{MsgID: mid, Type: envelope.TypeGroup, FromSeq: seq, LamportTS: lam, Payload: payload}
		for _, p := range members {
			if p == m.self {
				continue
			}
			if err := m.ob.EnqueueTx(tx, p, env); err != nil {
				return err
			}
			fanout = append(fanout, p)
		}
		return nil
	})
	if err != nil {
		return envelope.MsgID{}, err
	}
	for _, p := range fanout {
		m.ob.Flush(p)
	}
	return mid, nil
}

// Groups — все группы (для UI).
func (m *Manager) Groups(ctx context.Context) ([]store.GroupRec, error) {
	return m.db.GroupList(ctx)
}

// Messages — история группы.
func (m *Manager) Messages(ctx context.Context, gid [32]byte, limit int) ([]store.Message, error) {
	return m.db.ListMessages(ctx, peer.ID(gid), limit)
}

// onGroupMessage — входящее групповое сообщение (надёжный конверт).
func (m *Manager) onGroupMessage(tx *store.Tx, from peer.ID, env *envelope.Envelope) error {
	msg, err := DecodeMsg(env.Payload)
	if err != nil {
		m.ctr.malformed.Add(1)
		return nil
	}
	rec, ok, err := tx.GroupGet(msg.GroupID)
	if err != nil {
		return err
	}
	if !ok {
		// Welcome ещё в пути: откат без ack — ретрай дотащит после него.
		m.ctr.keyMisses.Add(1)
		return errors.New("group: группа ещё неизвестна")
	}
	if rec.Left {
		m.ctr.refused.Add(1)
		return nil // нас удалили: молча есть
	}
	isMember, err := tx.GroupIsMember(msg.GroupID, from)
	if err != nil {
		return err
	}
	if !isMember {
		// Либо самозванец, либо членство новее нашего ещё едет — ретрай
		// рассудит (самозванцу гейт всё равно не даст ack-флуда).
		m.ctr.keyMisses.Add(1)
		return errors.New("group: отправитель не в известном нам составе")
	}
	raw, ok, err := tx.SenderKeyGet(msg.GroupID, from)
	if err != nil {
		return err
	}
	if !ok {
		// Ключ отправителя ещё едет: откат без ack — доставка переиграется.
		m.ctr.keyMisses.Add(1)
		return errors.New("group: ключ отправителя ещё не получен")
	}
	receiver, err := senderkey.UnmarshalReceiver(raw)
	if err != nil {
		return err
	}
	plain, err := receiver.Decrypt(msg.GroupID, msg.Generation, msg.N, msg.Ciphertext, msg.Signature[:])
	switch {
	case errors.Is(err, senderkey.ErrFutureKey):
		m.ctr.keyMisses.Add(1)
		return err // rekey едет — переиграть позже
	case err != nil:
		// Подпись/повтор/слишком старое: съесть со счётчиком.
		m.ctr.undecryptable.Add(1)
		return nil
	}
	if err := tx.SenderKeyPut(msg.GroupID, from, receiver.Marshal(), time.Now().UnixMilli()); err != nil {
		return err
	}
	if err := tx.LamportObserve(env.LamportTS); err != nil {
		return err
	}
	rowMsg := store.Message{
		Peer: peer.ID(msg.GroupID), MsgID: env.MsgID, Outgoing: false,
		FromSeq: env.FromSeq, Lamport: env.LamportTS, SentAt: time.Now().UnixMilli(),
		Status: store.StatusDelivered, Body: plain, Sender: from[:],
	}
	if _, err := tx.InsertMessage(&rowMsg); err != nil {
		return err
	}
	tx.AfterCommit(func() {
		if m.onMessage != nil {
			m.onMessage(rowMsg)
		}
	})
	return nil
}

// onWelcome — нас пригласили: членство, чужие ключи, свой ключ и его
// раздача всем участникам.
func (m *Manager) onWelcome(tx *store.Tx, from peer.ID, plain []byte) error {
	w, err := DecodeWelcome(plain)
	if err != nil {
		m.ctr.malformed.Add(1)
		return nil
	}
	if !w.Membership.Verify() || !w.Membership.Has(m.self) {
		m.ctr.refused.Add(1)
		return nil
	}
	now := time.Now().UnixMilli()
	gid := w.Membership.GroupID

	if rec, ok, err := tx.GroupGet(gid); err != nil {
		return err
	} else if ok {
		// Уже знаем группу: welcome как update (повтор или новая версия).
		if w.Membership.Version <= rec.Version {
			return nil
		}
		return m.applyMembership(tx, rec, w.Membership, now)
	}

	memBytes, err := EncodeMembership(w.Membership)
	if err != nil {
		return err
	}
	if err := tx.GroupPut(&store.GroupRec{
		GroupID: gid, Name: w.Membership.Name, AdminPub: w.Membership.AdminPub,
		Version: w.Membership.Version, Membership: memBytes,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		return err
	}
	if err := tx.GroupMembersSet(gid, w.Membership.Members); err != nil {
		return err
	}
	for _, k := range w.Keys {
		if k.Member == m.self || !w.Membership.Has(k.Member) {
			continue
		}
		r := senderkey.NewReceiver(k.Key)
		if err := tx.SenderKeyPut(gid, k.Member, r.Marshal(), now); err != nil {
			return err
		}
	}
	// Свой ключ и его раздача.
	if err := m.rekeyTx(tx, gid, w.Membership.Members, now); err != nil {
		return err
	}
	tx.AfterCommit(func() {
		m.ctr.welcomes.Add(1)
		m.wake()
	})
	_ = from
	return nil
}

// onUpdate — новая версия членства от админа.
func (m *Manager) onUpdate(tx *store.Tx, from peer.ID, plain []byte) error {
	next, err := DecodeUpdate(plain)
	if err != nil {
		m.ctr.malformed.Add(1)
		return nil
	}
	rec, ok, err := tx.GroupGet(next.GroupID)
	if err != nil {
		return err
	}
	if !ok {
		// Welcome ещё в пути: откат без ack — ретрай переиграет позже.
		m.ctr.keyMisses.Add(1)
		return errors.New("group: группа ещё неизвестна")
	}
	if next.Version <= rec.Version {
		return nil // повтор
	}
	return m.applyMembership(tx, rec, next, time.Now().UnixMilli())
}

// applyMembership применяет новую версию: проверка подписи против
// ЗАПОМНЕННОГО admin_pub, обновление состава, rekey при удалении.
func (m *Manager) applyMembership(tx *store.Tx, rec store.GroupRec, next Membership, nowMs int64) error {
	if next.AdminPub != rec.AdminPub || !next.Verify() {
		m.ctr.refused.Add(1)
		return nil
	}
	old, err := DecodeMembership(rec.Membership)
	if err != nil {
		return err
	}
	nextBytes, err := EncodeMembership(next)
	if err != nil {
		return err
	}

	rec.Version = next.Version
	rec.Membership = nextBytes
	rec.Name = next.Name
	rec.UpdatedAt = nowMs
	rec.Left = !next.Has(m.self)
	if err := tx.GroupPut(&rec); err != nil {
		return err
	}
	if err := tx.GroupMembersSet(next.GroupID, next.Members); err != nil {
		return err
	}

	var removed []peer.ID
	for _, p := range old.Members {
		if !next.Has(p) {
			removed = append(removed, p)
		}
	}
	var added []peer.ID
	for _, p := range next.Members {
		if !old.Has(p) && p != m.self {
			added = append(added, p)
		}
	}
	for _, p := range removed {
		if err := tx.SenderKeyDelete(next.GroupID, p); err != nil {
			return err
		}
	}
	if len(removed) > 0 && !rec.Left {
		// Удаление участника = обязательный rekey оставшихся.
		if err := m.rekeyTx(tx, next.GroupID, next.Members, nowMs); err != nil {
			return err
		}
		tx.AfterCommit(m.wake)
	}
	if len(added) > 0 && !rec.Left && len(removed) == 0 {
		// Новичкам наш ключ мог не достаться через welcome (админ мог ещё
		// не иметь его на руках) — раздаём текущую точку сами.
		if err := m.distributeSelfKey(tx, next.GroupID, added, nowMs); err != nil {
			return err
		}
		tx.AfterCommit(m.wake)
	}
	return nil
}

// distributeSelfKey раскладывает текущую точку своего ключа адресатам.
func (m *Manager) distributeSelfKey(tx *store.Tx, gid [32]byte, to []peer.ID, nowMs int64) error {
	raw, ok, err := tx.SenderKeyGet(gid, m.self)
	if err != nil || !ok {
		return err
	}
	sender, err := senderkey.UnmarshalSender(raw)
	if err != nil {
		return err
	}
	dist, err := EncodeKeyDist(gid, sender.Dist())
	if err != nil {
		return err
	}
	for _, p := range to {
		if err := tx.SealedOutboxAdd(p, uint8(envelope.TypeGroupKey), dist, nowMs); err != nil {
			return err
		}
	}
	return nil
}

// onKey — участник раздал свой sender key (свежий или rekey).
func (m *Manager) onKey(tx *store.Tx, from peer.ID, plain []byte) error {
	gid, dist, err := DecodeKeyDist(plain)
	if err != nil {
		m.ctr.malformed.Add(1)
		return nil
	}
	if _, ok, err := tx.GroupGet(gid); err != nil {
		return err
	} else if !ok {
		m.ctr.keyMisses.Add(1)
		return errors.New("group: группа ещё неизвестна")
	}
	isMember, err := tx.GroupIsMember(gid, from)
	if err != nil {
		return err
	}
	if !isMember {
		// Раздача могла обогнать новую версию членства — ретрай дотащит.
		m.ctr.keyMisses.Add(1)
		return errors.New("group: раздающий не в известном нам составе")
	}
	// Старое поколение не понижает текущее (replay раздачи).
	if raw, ok, err := tx.SenderKeyGet(gid, from); err != nil {
		return err
	} else if ok {
		if cur, err := senderkey.UnmarshalReceiver(raw); err == nil && dist.Generation < cur.Generation() {
			m.ctr.refused.Add(1)
			return nil
		}
	}
	r := senderkey.NewReceiver(dist)
	if err := tx.SenderKeyPut(gid, from, r.Marshal(), time.Now().UnixMilli()); err != nil {
		return err
	}
	m.ctr.keysReceived.Add(1)
	return nil
}
