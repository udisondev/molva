package ipc

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/udisondev/molva/blob"
	"github.com/udisondev/molva/chat"
	"github.com/udisondev/molva/contact"
	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/proto/ipcpb"
	"github.com/udisondev/molva/store"
)

const (
	// authTimeout — сколько ждём Hello первым кадром.
	authTimeout = 5 * time.Second
	// eventQueue — глубина очереди событий клиенту; переполнение — дроп
	// со счётчиком (UI пересинхронизируется запросами).
	eventQueue = 1024
	// listLimit — потолок выдачи истории за один запрос.
	listLimit = 500
)

// client — одно подключение UI: соединение и его очередь исходящих кадров.
type client struct {
	conn *websocket.Conn
	send chan []byte
	stop chan struct{}
	once sync.Once
}

func (c *client) close() { c.once.Do(func() { close(c.stop) }) }

// Server — IPC-сервер molvad: один активный UI-клиент (новое подключение
// замещает старое — рестарт renderer'а не убивает узел), команды против
// подсистем ядра, события — из их колбэков. Слушает только loopback.
type Server struct {
	token []byte
	grace time.Duration

	chats    *chat.Manager
	contacts *contact.Manager
	files    *blob.Manager
	db       *store.DB
	self     peer.ID

	ln net.Listener

	mu       sync.Mutex
	active   *client
	lastSeen time.Time

	idleOnce sync.Once
	idleC    chan struct{}

	ctr counters
}

// NewServer создаёт сервер с auth-токеном. grace — сколько живём без
// клиента до сигнала Idle (0 — жить вечно).
func NewServer(token []byte, grace time.Duration) *Server {
	return &Server{
		token: token,
		grace: grace,
		idleC: make(chan struct{}),
	}
}

// Bind подключает подсистемы ядра (до Run).
func (s *Server) Bind(chats *chat.Manager, contacts *contact.Manager, files *blob.Manager, db *store.DB, self peer.ID) {
	s.chats = chats
	s.contacts = contacts
	s.files = files
	s.db = db
	s.self = self
}

// Listen открывает loopback-листенер; addr вида "127.0.0.1:0".
// Возвращает фактический адрес.
func (s *Server) Listen(addr string) (string, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host != "127.0.0.1" {
		return "", fmt.Errorf("ipc: слушаем только 127.0.0.1, не %q", addr)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("ipc: listen: %w", err)
	}
	s.ln = ln
	return ln.Addr().String(), nil
}

// Run обслуживает подключения до отмены ctx.
func (s *Server) Run(ctx context.Context) error {
	srv := &http.Server{Handler: s}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
		case <-done:
		}
		_ = srv.Close()
	}()
	if s.grace > 0 {
		go s.idleWatch(ctx)
	}
	s.mu.Lock()
	s.lastSeen = time.Now()
	s.mu.Unlock()
	err := srv.Serve(s.ln)
	if ctx.Err() != nil || errors.Is(err, http.ErrServerClosed) {
		return ctx.Err()
	}
	return err
}

// Idle сигналит, когда UI отсутствует дольше grace-периода — main гасит
// узел («фоновый демон без UI» — не цель v1).
func (s *Server) Idle() <-chan struct{} { return s.idleC }

func (s *Server) idleWatch(ctx context.Context) {
	ticker := time.NewTicker(s.grace / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		s.mu.Lock()
		idle := s.active == nil && time.Since(s.lastSeen) > s.grace
		s.mu.Unlock()
		if idle {
			s.idleOnce.Do(func() { close(s.idleC) })
			return
		}
	}
}

// ServeHTTP — upgrade, аутентификация первым кадром, петли чтения/записи.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Origin не значим: защита — loopback + токен (Electron шлёт file://).
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusInternalError, "")
	conn.SetReadLimit(MaxFrameLen + 1024)

	actx, cancel := context.WithTimeout(r.Context(), authTimeout)
	ok := s.auth(actx, conn)
	cancel()
	if !ok {
		s.ctr.authFailures.Add(1)
		conn.Close(websocket.StatusPolicyViolation, "auth")
		return
	}

	cl := &client{
		conn: conn,
		send: make(chan []byte, eventQueue),
		stop: make(chan struct{}),
	}
	s.attach(cl)
	defer s.detach(cl)

	go s.writeLoop(r.Context(), cl)
	s.readLoop(r.Context(), cl)
}

func (s *Server) auth(ctx context.Context, conn *websocket.Conn) bool {
	typ, data, err := conn.Read(ctx)
	if err != nil || typ != websocket.MessageBinary {
		return false
	}
	f, err := DecodeFrame(data)
	if err != nil {
		return false
	}
	hello := f.GetHello()
	if hello == nil {
		return false
	}
	return subtle.ConstantTimeCompare(hello.Token, s.token) == 1
}

func (s *Server) attach(cl *client) {
	s.mu.Lock()
	old := s.active
	s.active = cl
	s.lastSeen = time.Now()
	s.mu.Unlock()
	if old != nil {
		old.close()
		_ = old.conn.Close(websocket.StatusPolicyViolation, "replaced")
	}
}

func (s *Server) detach(cl *client) {
	cl.close()
	s.mu.Lock()
	if s.active == cl {
		s.active = nil
		s.lastSeen = time.Now()
	}
	s.mu.Unlock()
}

func (s *Server) writeLoop(ctx context.Context, cl *client) {
	for {
		select {
		case <-cl.stop:
			return
		case <-ctx.Done():
			return
		case b := <-cl.send:
			wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := cl.conn.Write(wctx, websocket.MessageBinary, b)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func (s *Server) readLoop(ctx context.Context, cl *client) {
	for {
		typ, data, err := cl.conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageBinary {
			continue
		}
		f, err := DecodeFrame(data)
		if err != nil {
			s.ctr.malformed.Add(1)
			continue
		}
		cmd := f.GetCommand()
		if cmd == nil {
			s.ctr.malformed.Add(1)
			continue
		}
		res := s.handle(ctx, cmd)
		s.push(cl, &ipcpb.Event{Kind: &ipcpb.Event_CommandResult{CommandResult: res}})
	}
}

// push кладёт событие в очередь конкретного клиента.
func (s *Server) push(cl *client, ev *ipcpb.Event) {
	b, err := EncodeFrame(&ipcpb.Frame{Kind: &ipcpb.Frame_Event{Event: ev}})
	if err != nil {
		s.ctr.eventsDropped.Add(1)
		return
	}
	select {
	case cl.send <- b:
	default:
		s.ctr.eventsDropped.Add(1)
	}
}

// emit шлёт событие активному клиенту; без клиента — дроп со счётчиком
// (состояние живёт в store, UI пересинхронизируется при подключении).
func (s *Server) emit(ev *ipcpb.Event) {
	s.mu.Lock()
	cl := s.active
	s.mu.Unlock()
	if cl == nil {
		s.ctr.eventsDropped.Add(1)
		return
	}
	s.push(cl, ev)
}

// Колбэки ядра — проводка в app.Config.

// OnMessage — принято входящее сообщение.
func (s *Server) OnMessage(m store.Message) {
	s.emit(&ipcpb.Event{Kind: &ipcpb.Event_MessageReceived{
		MessageReceived: &ipcpb.MessageReceived{Message: toPBMessage(m)},
	}})
}

// OnDelivered — наш конверт подтверждён получателем.
func (s *Server) OnDelivered(p peer.ID, mid envelope.MsgID) {
	s.emit(&ipcpb.Event{Kind: &ipcpb.Event_MessageDelivered{
		MessageDelivered: &ipcpb.MessageDelivered{Peer: p[:], MsgId: mid[:]},
	}})
}

// OnContactRequest — входящий запрос знакомства.
func (s *Server) OnContactRequest(p peer.ID, suggested string) {
	s.emit(&ipcpb.Event{Kind: &ipcpb.Event_ContactRequested{
		ContactRequested: &ipcpb.ContactRequested{Peer: p[:], SuggestedAlias: suggested},
	}})
}

// OnContactAccept — наше знакомство принято.
func (s *Server) OnContactAccept(p peer.ID) {
	s.emit(&ipcpb.Event{Kind: &ipcpb.Event_ContactAccepted{
		ContactAccepted: &ipcpb.ContactAccepted{Peer: p[:]},
	}})
}

// OnPresence — контакт сменил статус присутствия.
func (s *Server) OnPresence(p peer.ID, online bool) {
	s.emit(&ipcpb.Event{Kind: &ipcpb.Event_PresenceChanged{
		PresenceChanged: &ipcpb.PresenceChanged{Peer: p[:], Online: online},
	}})
}

// OnFileOffered — контакт предложил файл (приём начинается сам).
func (s *Server) OnFileOffered(p peer.ID, man blob.Manifest) {
	s.emit(&ipcpb.Event{Kind: &ipcpb.Event_FileOffered{
		FileOffered: &ipcpb.FileOffered{Peer: p[:], FileId: man.FileID[:], Name: man.Name, Size: man.Size},
	}})
}

// OnFileProgress — продвижение приёма файла.
func (s *Server) OnFileProgress(fileID [16]byte, have, total int) {
	s.emit(&ipcpb.Event{Kind: &ipcpb.Event_FileProgress{
		FileProgress: &ipcpb.FileProgress{FileId: fileID[:], Have: uint32(have), Total: uint32(total)},
	}})
}

// OnFileDone — файл принят и проверен.
func (s *Server) OnFileDone(fileID [16]byte, path string) {
	s.emit(&ipcpb.Event{Kind: &ipcpb.Event_FileDone{
		FileDone: &ipcpb.FileDone{FileId: fileID[:], Path: path},
	}})
}

// handle исполняет команду UI и собирает результат.
func (s *Server) handle(ctx context.Context, cmd *ipcpb.Command) *ipcpb.CommandResult {
	res := &ipcpb.CommandResult{Id: cmd.Id}
	err := s.dispatch(ctx, cmd, res)
	if err != nil {
		res.Error = err.Error()
	}
	return res
}

func (s *Server) dispatch(ctx context.Context, cmd *ipcpb.Command, res *ipcpb.CommandResult) error {
	switch k := cmd.Kind.(type) {
	case *ipcpb.Command_SendText:
		p, err := parsePeer(k.SendText.Peer)
		if err != nil {
			return err
		}
		mid, err := s.chats.SendText(ctx, p, k.SendText.Text)
		if err != nil {
			return err
		}
		m, ok, err := s.db.GetMessage(ctx, p, mid, true)
		if err != nil || !ok {
			return fmt.Errorf("ipc: отправленное не нашлось: %w", err)
		}
		res.Data = &ipcpb.CommandResult_Sent{Sent: &ipcpb.SentMessage{Message: toPBMessage(m)}}
		return nil

	case *ipcpb.Command_AcceptContact:
		p, err := parsePeer(k.AcceptContact.Peer)
		if err != nil {
			return err
		}
		return s.contacts.Accept(ctx, p)

	case *ipcpb.Command_RejectContact:
		p, err := parsePeer(k.RejectContact.Peer)
		if err != nil {
			return err
		}
		return s.contacts.Reject(ctx, p)

	case *ipcpb.Command_BlockContact:
		p, err := parsePeer(k.BlockContact.Peer)
		if err != nil {
			return err
		}
		return s.contacts.Block(ctx, p)

	case *ipcpb.Command_UnblockContact:
		p, err := parsePeer(k.UnblockContact.Peer)
		if err != nil {
			return err
		}
		return s.contacts.Unblock(ctx, p)

	case *ipcpb.Command_SetAlias:
		p, err := parsePeer(k.SetAlias.Peer)
		if err != nil {
			return err
		}
		return s.contacts.SetAlias(ctx, p, k.SetAlias.Alias)

	case *ipcpb.Command_DeleteMessage:
		p, err := parsePeer(k.DeleteMessage.Peer)
		if err != nil {
			return err
		}
		mid, err := parseMsgID(k.DeleteMessage.MsgId)
		if err != nil {
			return err
		}
		return s.chats.Delete(ctx, p, mid)

	case *ipcpb.Command_AddContact:
		_, err := s.contacts.AddByInvite(ctx, k.AddContact.Invite)
		return err

	case *ipcpb.Command_ListChats:
		chats, err := s.listChats(ctx)
		if err != nil {
			return err
		}
		res.Data = &ipcpb.CommandResult_Chats{Chats: chats}
		return nil

	case *ipcpb.Command_ListMessages:
		p, err := parsePeer(k.ListMessages.Peer)
		if err != nil {
			return err
		}
		limit := int(k.ListMessages.Limit)
		if limit <= 0 || limit > listLimit {
			limit = listLimit
		}
		msgs, err := s.db.ListMessages(ctx, p, limit)
		if err != nil {
			return err
		}
		list := &ipcpb.MessageList{}
		for _, m := range msgs {
			list.Messages = append(list.Messages, toPBMessage(m))
		}
		res.Data = &ipcpb.CommandResult_Messages{Messages: list}
		return nil

	case *ipcpb.Command_MyInvite:
		res.Data = &ipcpb.CommandResult_Invite{Invite: &ipcpb.Invite{
			Invite: s.contacts.MyInvite(k.MyInvite.Alias),
		}}
		return nil

	case *ipcpb.Command_OfferFile:
		p, err := parsePeer(k.OfferFile.Peer)
		if err != nil {
			return err
		}
		_, err = s.files.Offer(ctx, p, k.OfferFile.Path)
		return err

	default:
		return errors.New("ipc: неизвестная команда")
	}
}

func (s *Server) listChats(ctx context.Context) (*ipcpb.ChatList, error) {
	infos, err := s.contacts.Contacts(ctx)
	if err != nil {
		return nil, err
	}
	out := &ipcpb.ChatList{}
	for _, info := range infos {
		ch := &ipcpb.Chat{
			Peer:   info.Peer[:],
			Alias:  info.Alias,
			Online: s.contacts.Online(info.Peer),
			State:  toPBState(info.State),
		}
		if last, ok, err := s.db.LastMessage(ctx, info.Peer); err == nil && ok {
			ch.LastMessage = toPBMessage(last)
		}
		out.Chats = append(out.Chats, ch)
	}
	return out, nil
}

func toPBState(s store.PeerState) ipcpb.Chat_State {
	switch s {
	case store.PeerPendingOut:
		return ipcpb.Chat_STATE_PENDING_OUT
	case store.PeerPendingIn:
		return ipcpb.Chat_STATE_PENDING_IN
	case store.PeerContact:
		return ipcpb.Chat_STATE_CONTACT
	case store.PeerBlocked:
		return ipcpb.Chat_STATE_BLOCKED
	default:
		return ipcpb.Chat_STATE_UNSPECIFIED
	}
}

func toPBMessage(m store.Message) *ipcpb.Message {
	pm := &ipcpb.Message{
		MsgId:    m.MsgID[:],
		Peer:     m.Peer[:],
		Outgoing: m.Outgoing,
		Deleted:  m.Deleted,
		SentAt:   m.SentAt,
	}
	if !m.Deleted && m.Body != nil {
		pm.Text = string(m.Body)
	}
	switch m.Status {
	case store.StatusQueued:
		pm.Status = ipcpb.Message_STATUS_QUEUED
	case store.StatusSent:
		pm.Status = ipcpb.Message_STATUS_SENT
	case store.StatusDelivered:
		pm.Status = ipcpb.Message_STATUS_DELIVERED
	}
	return pm
}

func parsePeer(b []byte) (peer.ID, error) {
	if len(b) != peer.IDLen {
		return peer.ID{}, errors.New("ipc: кривой peer id")
	}
	var p peer.ID
	copy(p[:], b)
	return p, nil
}

func parseMsgID(b []byte) (envelope.MsgID, error) {
	if len(b) != envelope.MsgIDLen {
		return envelope.MsgID{}, errors.New("ipc: кривой msg id")
	}
	var m envelope.MsgID
	copy(m[:], b)
	return m, nil
}
