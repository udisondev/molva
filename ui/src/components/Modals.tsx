import { useEffect, useState } from "react";
import {
  addContact,
  copyText,
  loadInvite,
  setModal,
  useStore,
} from "../state/store";

// Модалка «мой инвайт»: строка с предлагаемым именем, копирование кликом.
export function InviteModal() {
  const { invite } = useStore();
  const [alias, setAlias] = useState("");

  useEffect(() => {
    void loadInvite(alias);
    // Перегенерация при смене имени — лёгкая команда к ядру.
  }, [alias]);

  return (
    <div className="backdrop" onClick={() => setModal(null)}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h2>Мой инвайт</h2>
        <div className="hint">
          Передайте эту строку собеседнику любым каналом — мессенджером,
          бумажкой, голосом. Внутри только ваш публичный позывной.
        </div>
        <div>
          <label style={{ fontFamily: "var(--mono)", fontSize: 10, letterSpacing: "0.15em", color: "var(--faint)" }}>
            ПРЕДСТАВИТЬСЯ КАК (необязательно)
          </label>
          <input value={alias} onChange={(e) => setAlias(e.target.value)} placeholder="имя-подсказка" />
        </div>
        <div className="invite-box" title="Скопировать" onClick={() => invite && void copyText(invite, "ИНВАЙТ СКОПИРОВАН")}>
          {invite || "…"}
        </div>
        <div className="row">
          <button onClick={() => setModal(null)}>закрыть</button>
        </div>
      </div>
    </div>
  );
}

// Модалка «выйти на связь»: вставка чужого инвайта.
export function AddContactModal() {
  const [value, setValue] = useState("");
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    setBusy(true);
    const e = await addContact(value.trim());
    setBusy(false);
    if (e) {
      setErr(e);
      return;
    }
    setModal(null);
  };

  return (
    <div className="backdrop" onClick={() => setModal(null)}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h2>Выйти на связь</h2>
        <div className="hint">
          Вставьте инвайт-строку вида molva://add/… — запрос знакомства уйдёт,
          когда оба узла окажутся в эфире.
        </div>
        <input
          autoFocus
          value={value}
          onChange={(e) => {
            setValue(e.target.value);
            setErr(null);
          }}
          onKeyDown={(e) => e.key === "Enter" && void submit()}
          placeholder="molva://add/…"
        />
        {err && <div className="err">{err}</div>}
        <div className="row">
          <button onClick={() => setModal(null)}>отмена</button>
          <button className="primary" disabled={!value.trim() || busy} onClick={() => void submit()}>
            отправить запрос
          </button>
        </div>
      </div>
    </div>
  );
}
