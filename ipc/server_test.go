package ipc

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/udisondev/molva/app"
	"github.com/udisondev/molva/contact"
	"github.com/udisondev/molva/media"
	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/proto/ipcpb"
	"github.com/udisondev/nodenet/identity"
	"github.com/udisondev/nodenet/transport"
	"github.com/udisondev/nodenet/transport/mem"
)

var testToken = []byte("тестовый-токен-0123456789abcdef")

// startServer поднимает настоящий core (mem-транспорт, один узел) и IPC
// поверх; возвращает адрес WS.
func startServer(t *testing.T) (*Server, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	seed := [identity.SeedLen]byte{42}
	id := identity.FromSeed(seed)
	hub := mem.NewHub()
	tr, err := hub.New(id.ID(), transport.Addr{Net: "mem", Endpoint: "solo"})
	if err != nil {
		t.Fatal(err)
	}

	srv := NewServer(testToken, 0)
	core, err := app.New(app.Config{
		Seed:               seed,
		DataDir:            t.TempDir(),
		Transport:          tr,
		OnMessage:          srv.OnMessage,
		OnDelivered:        srv.OnDelivered,
		OnContactAdded:     srv.OnContactAdded,
		OnPresence:         srv.OnPresence,
		OnFileOffered:      srv.OnFileOffered,
		OnFileProgress:     srv.OnFileProgress,
		OnFileDone:         srv.OnFileDone,
		OnCallIncoming:     srv.OnCallIncoming,
		OnCallState:        srv.OnCallState,
		OnMediaFrame:       srv.PushMedia,
		OnCallReconnecting: srv.OnCallReconnecting,
		OnPreset:           func(l media.Preset) { srv.OnPreset(uint8(l)) },
	})
	if err != nil {
		t.Fatal(err)
	}
	srv.Bind(core.Chats(), core.Contacts(), core.Files(), core.Calls(), core, core.Media().Send, core.ReflexiveAddr, core.Store(), peer.ID(core.ID()))

	coreDone := make(chan struct{})
	go func() { defer close(coreDone); _ = core.Run(ctx) }()
	t.Cleanup(func() { cancel(); _ = tr.Close(); <-coreDone })

	addr, err := srv.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Run(ctx) }()
	return srv, addr
}

func dial(t *testing.T, addr string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws://"+addr, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.SetReadLimit(MaxMediaPayload + 4096)
	return conn
}

func send(t *testing.T, conn *websocket.Conn, f *ipcpb.Frame) {
	t.Helper()
	b, err := EncodeFrame(f)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageBinary, b); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func hello(t *testing.T, conn *websocket.Conn, token []byte) {
	t.Helper()
	send(t, conn, &ipcpb.Frame{Kind: &ipcpb.Frame_Hello{Hello: &ipcpb.Hello{Token: token}}})
}

// recvEvent читает кадры до события; падает по таймауту.
func recvEvent(t *testing.T, conn *websocket.Conn) *ipcpb.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if typ != websocket.MessageBinary {
			continue
		}
		f, err := DecodeFrame(data)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if ev := f.GetEvent(); ev != nil {
			return ev
		}
	}
}

func command(t *testing.T, conn *websocket.Conn, id uint64, kind any) *ipcpb.CommandResult {
	t.Helper()
	cmd := &ipcpb.Command{Id: id}
	switch k := kind.(type) {
	case *ipcpb.Command_MyInvite:
		cmd.Kind = k
	case *ipcpb.Command_AddContact:
		cmd.Kind = k
	case *ipcpb.Command_ListChats:
		cmd.Kind = k
	case *ipcpb.Command_SendText:
		cmd.Kind = k
	case *ipcpb.Command_MyIdentity:
		cmd.Kind = k
	case *ipcpb.Command_ListBootstrap:
		cmd.Kind = k
	case *ipcpb.Command_AddBootstrap:
		cmd.Kind = k
	case *ipcpb.Command_RemoveBootstrap:
		cmd.Kind = k
	default:
		t.Fatalf("неизвестный вид команды: %T", kind)
	}
	send(t, conn, &ipcpb.Frame{Kind: &ipcpb.Frame_Command{Command: cmd}})
	for {
		ev := recvEvent(t, conn)
		res := ev.GetCommandResult()
		if res == nil || res.Id != id {
			continue
		}
		return res
	}
}

// Неверный токен рвёт соединение и виден в счётчике; верный — пускает.
func TestAuth(t *testing.T) {
	srv, addr := startServer(t)

	bad := dial(t, addr)
	hello(t, bad, []byte("не тот"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, _, err := bad.Read(ctx); err == nil {
		t.Fatal("сервер обязан разорвать соединение с неверным токеном")
	}
	if got := srv.Stats().AuthFailures; got != 1 {
		t.Fatalf("AuthFailures = %d, want 1", got)
	}

	good := dial(t, addr)
	defer good.Close(websocket.StatusNormalClosure, "")
	hello(t, good, testToken)
	res := command(t, good, 1, &ipcpb.Command_MyInvite{MyInvite: &ipcpb.MyInvite{Alias: "Я"}})
	if res.Error != "" {
		t.Fatalf("MyInvite: %s", res.Error)
	}
	inv := res.GetInvite().GetInvite()
	if !strings.HasPrefix(inv, "molva://add/") {
		t.Fatalf("кривой инвайт: %q", inv)
	}
}

// Команды против ядра: добавленный по инвайту сразу контакт — писать
// можно немедленно, без одобрения второй стороны.
func TestCommandsAgainstCore(t *testing.T) {
	_, addr := startServer(t)
	conn := dial(t, addr)
	defer conn.Close(websocket.StatusNormalClosure, "")
	hello(t, conn, testToken)

	// Чужой инвайт (детерминированная вторая личность).
	other := identity.FromSeed([identity.SeedLen]byte{77})
	invite := contact.EncodeInvite(peer.ID(other.ID()), "Сосед")

	res := command(t, conn, 1, &ipcpb.Command_AddContact{AddContact: &ipcpb.AddContact{Invite: invite}})
	if res.Error != "" {
		t.Fatalf("AddContact: %s", res.Error)
	}

	res = command(t, conn, 2, &ipcpb.Command_ListChats{ListChats: &ipcpb.ListChats{}})
	if res.Error != "" {
		t.Fatalf("ListChats: %s", res.Error)
	}
	chats := res.GetChats().GetChats()
	if len(chats) != 1 || chats[0].State != ipcpb.Chat_STATE_CONTACT || chats[0].Alias != "Сосед" {
		t.Fatalf("список чатов: %+v", chats)
	}

	// Писать можно сразу: текст встаёт в очередь ожидания сессии/доставки.
	res = command(t, conn, 3, &ipcpb.Command_SendText{SendText: &ipcpb.SendText{
		Peer: chats[0].Peer, Text: "сразу после инвайта",
	}})
	if res.Error != "" {
		t.Fatalf("SendText свежему контакту: %s", res.Error)
	}
	if res.GetSent().GetMessage().GetStatus() != ipcpb.Message_STATUS_QUEUED {
		t.Fatalf("статус: %v, want QUEUED", res.GetSent().GetMessage().GetStatus())
	}
}

// Свой NodeID и управление бутстрапом через IPC.
func TestIdentityAndBootstrap(t *testing.T) {
	_, addr := startServer(t)
	conn := dial(t, addr)
	defer conn.Close(websocket.StatusNormalClosure, "")
	hello(t, conn, testToken)

	// NodeID — детерминированный seed{42}.
	res := command(t, conn, 1, &ipcpb.Command_MyIdentity{MyIdentity: &ipcpb.MyIdentity{}})
	if res.Error != "" {
		t.Fatalf("MyIdentity: %s", res.Error)
	}
	want := identity.FromSeed([identity.SeedLen]byte{42}).ID().String()
	if got := res.GetIdentity().GetNodeId(); got != want {
		t.Fatalf("NodeID = %s, want %s", got, want)
	}

	// Список бутстрапа пуст.
	res = command(t, conn, 2, &ipcpb.Command_ListBootstrap{ListBootstrap: &ipcpb.ListBootstrap{}})
	if res.Error != "" || len(res.GetBootstrap().GetEntries()) != 0 {
		t.Fatalf("ListBootstrap: %s %v", res.Error, res.GetBootstrap().GetEntries())
	}

	// Кривая строка отвергается.
	res = command(t, conn, 3, &ipcpb.Command_AddBootstrap{AddBootstrap: &ipcpb.AddBootstrap{Entry: "мусор"}})
	if res.Error == "" {
		t.Fatal("кривой бутстрап обязан вернуть ошибку")
	}

	// Валидная точка входа добавляется и попадает в список.
	other := identity.FromSeed([identity.SeedLen]byte{99}).ID().String()
	entry := other + "@127.0.0.1:7777"
	res = command(t, conn, 4, &ipcpb.Command_AddBootstrap{AddBootstrap: &ipcpb.AddBootstrap{Entry: entry}})
	if res.Error != "" {
		t.Fatalf("AddBootstrap: %s", res.Error)
	}
	if got := res.GetBootstrap().GetEntries(); len(got) != 1 || got[0] != entry {
		t.Fatalf("после добавления: %v", got)
	}

	// Удаление убирает из списка.
	res = command(t, conn, 5, &ipcpb.Command_RemoveBootstrap{RemoveBootstrap: &ipcpb.RemoveBootstrap{Entry: entry}})
	if res.Error != "" || len(res.GetBootstrap().GetEntries()) != 0 {
		t.Fatalf("RemoveBootstrap: %s %v", res.Error, res.GetBootstrap().GetEntries())
	}
}

// Новое подключение замещает старое: рестарт renderer'а не теряет узел.
func TestReconnectReplaces(t *testing.T) {
	_, addr := startServer(t)

	first := dial(t, addr)
	hello(t, first, testToken)
	res := command(t, first, 1, &ipcpb.Command_ListChats{ListChats: &ipcpb.ListChats{}})
	if res.Error != "" {
		t.Fatal(res.Error)
	}

	second := dial(t, addr)
	defer second.Close(websocket.StatusNormalClosure, "")
	hello(t, second, testToken)
	res = command(t, second, 2, &ipcpb.Command_ListChats{ListChats: &ipcpb.ListChats{}})
	if res.Error != "" {
		t.Fatal(res.Error)
	}

	// Старое соединение закрыто сервером.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		if _, _, err := first.Read(ctx); err != nil {
			break
		}
	}
}
