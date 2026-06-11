import { useEffect, useRef } from "react";
import { Message_Status } from "../gen/ipc";
import { MessageVM, deleteMessage, useStore } from "../state/store";

function mark(m: MessageVM): { sym: string; cls: string } {
  if (!m.outgoing) return { sym: "", cls: "" };
  switch (m.status) {
    case Message_Status.STATUS_DELIVERED:
      return { sym: "✓", cls: "delivered" };
    case Message_Status.STATUS_SENT:
      return { sym: "→", cls: "" };
    default:
      return { sym: "⋯", cls: "queued" };
  }
}

function hhmm(ts: number): string {
  const d = new Date(ts);
  return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
}

function dayKey(ts: number): string {
  const d = new Date(ts);
  return d.toLocaleDateString("ru-RU", { day: "numeric", month: "long" });
}

export function Thread() {
  const { selected, threads } = useStore();
  const msgs = (selected && threads[selected]) || [];
  const endRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    endRef.current?.scrollIntoView({ block: "end" });
  }, [selected, msgs.length]);

  let lastDay = "";
  return (
    <div className="thread">
      {msgs.map((m) => {
        const day = dayKey(m.sentAt);
        const sep = day !== lastDay;
        lastDay = day;
        const mk = mark(m);
        return (
          <div key={m.key}>
            {sep && <div className="daysep">— {day} —</div>}
            <div className={"entry" + (m.outgoing ? " out" : "")}>
              <span className="gutter">{hhmm(m.sentAt)}</span>
              <span className={"bubble" + (m.deleted ? " deleted" : "")}>
                {m.deleted ? "стёрто локально" : m.text}
                {!m.deleted && (
                  <button className="del" title="Стереть у себя" onClick={() => void deleteMessage(m)}>
                    ✕
                  </button>
                )}
              </span>
              {mk.sym && <span className={"mark " + mk.cls}>{mk.sym}</span>}
            </div>
          </div>
        );
      })}
      {msgs.length === 0 && (
        <div className="empty">
          <span>
            <span className="sigil">⌁</span>
            журнал пуст
          </span>
        </div>
      )}
      <div ref={endRef} />
    </div>
  );
}
