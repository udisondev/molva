import { useRef, useState } from "react";
import { Chat_State } from "../gen/ipc";
import { offerFile, sendText, useStore } from "../state/store";

export function Composer() {
  const { selected, chats } = useStore();
  const [text, setText] = useState("");
  const areaRef = useRef<HTMLTextAreaElement>(null);
  const chat = chats.find((c) => c.peerHex === selected);
  const canWrite = !!chat && chat.state !== Chat_State.STATE_BLOCKED;

  const submit = () => {
    const t = text.trim();
    if (!t) return;
    void sendText(t);
    setText("");
    if (areaRef.current) areaRef.current.style.height = "auto";
  };

  return (
    <div className="composer">
      <button
        className="ghost"
        title="Передать файл"
        disabled={!canWrite}
        onClick={() => void offerFile()}
      >
        ⎙
      </button>
      <span className="prompt">▸</span>
      <textarea
        ref={areaRef}
        rows={1}
        value={text}
        placeholder={canWrite ? "сообщение… (Enter — в эфир)" : "корреспондент в чёрном списке"}
        disabled={!canWrite}
        onChange={(e) => {
          setText(e.target.value);
          e.target.style.height = "auto";
          e.target.style.height = `${Math.min(e.target.scrollHeight, 140)}px`;
        }}
        onKeyDown={(e) => {
          if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            submit();
          }
        }}
      />
      <button className="primary" onClick={submit} disabled={!canWrite || !text.trim()}>
        передать
      </button>
    </div>
  );
}
