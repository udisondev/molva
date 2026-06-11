import { useState } from "react";
import { Chat_State } from "../gen/ipc";
import { blockContact, copyText, setAlias, unblockContact, useStore } from "../state/store";

export function ContactCard() {
  const { selected, chats } = useStore();
  const chat = chats.find((c) => c.peerHex === selected);
  const [alias, setAliasInput] = useState(chat?.alias ?? "");

  if (!chat) return null;
  const blocked = chat.state === Chat_State.STATE_BLOCKED;

  return (
    <aside className="drawer">
      <h3>Карточка корреспондента</h3>

      <div>
        <label>Позывной (NodeID)</label>
        <div
          className="fullid"
          title="Скопировать"
          onClick={() => void copyText(chat.peerHex)}
        >
          {chat.peerHex}
        </div>
      </div>

      <div>
        <label>Локальный алиас</label>
        <div style={{ display: "flex", gap: 6 }}>
          <input
            value={alias}
            onChange={(e) => setAliasInput(e.target.value)}
            placeholder="как записать в журнале"
            style={{ flex: 1 }}
          />
          <button
            onClick={() => void setAlias(chat, alias)}
            disabled={alias === chat.alias}
          >
            ок
          </button>
        </div>
      </div>

      <div style={{ flex: 1 }} />

      {blocked ? (
        <button onClick={() => void unblockContact(chat)}>разблокировать</button>
      ) : (
        <button className="danger" onClick={() => void blockContact(chat)}>
          заблокировать
        </button>
      )}
      <div className="hint" style={{ fontSize: 11, color: "var(--faint)", lineHeight: 1.5 }}>
        Блокировка молчалива: для него вы — вечный офлайн. История остаётся у
        вас, очередь к нему стирается.
      </div>
    </aside>
  );
}
