import { Chat_State } from "./gen/ipc";
import { CallBar } from "./components/CallBar";
import { ChatList } from "./components/ChatList";
import { Composer } from "./components/Composer";
import { ContactCard } from "./components/ContactCard";
import { Thread } from "./components/Thread";
import { AddContactModal, InviteModal } from "./components/Modals";
import { Settings } from "./components/Settings";
import { setModal, startCall, toggleDrawer, useStore } from "./state/store";

export function App() {
  const { connected, selected, chats, drawer, modal, toast } = useStore();
  const chat = chats.find((c) => c.peerHex === selected);

  return (
    <>
      <header className="topbar">
        <span className={"wordmark" + (connected ? "" : " offline")}>MOLVA</span>
        <span className="spacer" />
        <button className="ghost" onClick={() => setModal("invite")}>
          мой инвайт
        </button>
        <button className="primary" onClick={() => setModal("add")}>
          + на связь
        </button>
        <button className="ghost" title="Настройки" onClick={() => setModal("settings")}>
          ⚙
        </button>
      </header>
      <CallBar />

      <div className="body">
        <nav className="rail">
          <div className="rail-head">эфир</div>
          <ChatList />
        </nav>

        <main className="main">
          {chat ? (
            <>
              <div className="thread-head">
                <span className={"lamp" + (chat.online ? " on" : "")} />
                <span className="who">{chat.alias.trim() || chat.peerHex.slice(0, 12) + "…"}</span>
                <span className="id" onClick={toggleDrawer} title="Карточка корреспондента">
                  {chat.peerHex.slice(0, 16)}…
                </span>
                <span className="spacer" style={{ flex: 1 }} />
                {chat.state !== Chat_State.STATE_BLOCKED && (
                  <button className="ghost" title="Позвонить" onClick={() => void startCall()}>
                    ☎
                  </button>
                )}
                <button className="ghost" onClick={toggleDrawer}>
                  ⚙
                </button>
              </div>
              <Thread />
              <Composer />
            </>
          ) : (
            <div className="empty">
              <span>
                <span className="sigil">⌖</span>
                выберите корреспондента
                <br />
                или обменяйтесь инвайтами
              </span>
            </div>
          )}
        </main>

        {drawer && chat && <ContactCard key={chat.peerHex} />}
      </div>

      {modal === "invite" && <InviteModal />}
      {modal === "add" && <AddContactModal />}
      {modal === "settings" && <Settings />}
      {toast && <div className="toast">{toast}</div>}
    </>
  );
}
