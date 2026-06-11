import { Chat_State } from "../gen/ipc";
import { ChatVM, acceptContact, rejectContact, selectChat, useStore } from "../state/store";
import { Avatar } from "./Avatar";

function rowClass(c: ChatVM, selected: boolean): string {
  const cls = ["chatrow"];
  if (selected) cls.push("selected");
  if (c.state === Chat_State.STATE_PENDING_IN) cls.push("pending-in");
  if (c.state === Chat_State.STATE_BLOCKED) cls.push("blocked");
  return cls.join(" ");
}

function displayName(c: ChatVM): string {
  return c.alias.trim() || c.peerHex.slice(0, 12) + "…";
}

export function ChatList() {
  const { chats, selected } = useStore();
  return (
    <div className="chatlist">
      {chats.map((c) => (
        <button
          key={c.peerHex}
          className={rowClass(c, selected === c.peerHex)}
          onClick={() => selectChat(c.peerHex)}
        >
          <Avatar peerHex={c.peerHex} alias={c.alias} />
          <span className="meta">
            <span className="name">
              <span className={"lamp" + (c.online ? " on" : "")} />
              {displayName(c)}
              {c.state === Chat_State.STATE_PENDING_OUT && (
                <span className="tag gray">ЖДЁТ ОТВЕТА</span>
              )}
              {c.state === Chat_State.STATE_BLOCKED && <span className="tag red">БЛОК</span>}
              {c.unread > 0 && <span className="tag">{c.unread}</span>}
            </span>
            {c.state === Chat_State.STATE_PENDING_IN ? (
              <span className="preview">просится на связь</span>
            ) : (
              <span className="preview">{c.preview || "—"}</span>
            )}
          </span>
          {c.state === Chat_State.STATE_PENDING_IN && (
            <span className="row-actions">
              <button
                className="primary"
                onClick={(e) => {
                  e.stopPropagation();
                  void acceptContact(c);
                }}
              >
                ✓
              </button>
              <button
                className="danger"
                onClick={(e) => {
                  e.stopPropagation();
                  void rejectContact(c);
                }}
              >
                ✕
              </button>
            </span>
          )}
        </button>
      ))}
      {chats.length === 0 && (
        <div className="empty" style={{ padding: "40px 12px" }}>
          <span>
            <span className="sigil">◌</span>
            эфир пуст
            <br />
            обменяйтесь инвайтами
          </span>
        </div>
      )}
    </div>
  );
}
