import { CallEvent_State } from "../gen/ipc";
import { acceptCall, hangupCall, rejectCall, rejectCallAndBlock, useStore } from "../state/store";

// Панель звонка: входящий (принять/отклонить/отклонить-и-в-ЧС) или
// активный (повесить).
export function CallBar() {
  const { call, chats } = useStore();
  if (!call) return null;
  const chat = chats.find((c) => c.peerHex === call.peerHex);
  const who = chat?.alias.trim() || call.peerHex.slice(0, 12) + "…";

  let label = "";
  switch (call.state) {
    case CallEvent_State.STATE_RINGING_OUT:
      label = `вызываем ${who}…`;
      break;
    case CallEvent_State.STATE_RINGING_IN:
      label = `входящий вызов: ${who}`;
      break;
    case CallEvent_State.STATE_ACTIVE:
      label = call.reconnecting ? `связь с ${who} восстанавливается…` : `в эфире с ${who}`;
      break;
    default:
      return null;
  }

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 12,
        padding: "8px 16px",
        borderBottom: "1px solid var(--amber-dim)",
        background: "var(--amber-glow)",
        fontFamily: "var(--mono)",
        fontSize: 12,
      }}
    >
      <span style={{ color: "var(--amber)", textShadow: "0 0 10px var(--amber-glow)" }}>☎</span>
      <span style={{ flex: 1 }}>{label}</span>
      {call.state === CallEvent_State.STATE_RINGING_IN && (
        <>
          <button className="primary" onClick={() => void acceptCall()}>
            принять
          </button>
          <button className="danger" onClick={() => void rejectCall()}>
            отклонить
          </button>
          <button className="danger" title="Отклонить и заблокировать" onClick={() => void rejectCallAndBlock()}>
            в чёрный список
          </button>
        </>
      )}
      {call.state !== CallEvent_State.STATE_RINGING_IN && (
        <button className="danger" onClick={() => void hangupCall()}>
          повесить
        </button>
      )}
    </div>
  );
}
