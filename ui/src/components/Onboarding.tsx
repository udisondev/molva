import { useState } from "react";

// Онбординг: создать новую личность (показать фразу для бэкапа) или
// восстановить из 24 слов. По завершении — onReady (личность на диске).
export function Onboarding({ onReady }: { onReady: () => void }) {
  const [mode, setMode] = useState<"choose" | "create" | "restore">("choose");

  return (
    <div className="onboard">
      <div className="onboard-card">
        <div className="wordmark" style={{ fontSize: 22, textAlign: "center", marginBottom: 6 }}>
          MOLVA
        </div>
        <div className="onboard-sub">P2P-мессенджер без серверов</div>

        {mode === "choose" && (
          <div className="onboard-choose">
            <p className="hint" style={{ textAlign: "center", lineHeight: 1.6 }}>
              Ваша личность — это секретная фраза из 24 слов. Сервера и
              регистрации нет: фраза и есть ваш аккаунт и единственный бэкап.
            </p>
            <button className="primary big" onClick={() => setMode("create")}>
              Создать новую личность
            </button>
            <button className="big" onClick={() => setMode("restore")}>
              Восстановить из фразы
            </button>
          </div>
        )}

        {mode === "create" && <Create onReady={onReady} onBack={() => setMode("choose")} />}
        {mode === "restore" && <Restore onReady={onReady} onBack={() => setMode("choose")} />}
      </div>
    </div>
  );
}

function Create({ onReady, onBack }: { onReady: () => void; onBack: () => void }) {
  const [phrase, setPhrase] = useState<string | null>(null);
  const [nodeId, setNodeId] = useState("");
  const [confirmed, setConfirmed] = useState(false);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const generate = async () => {
    setBusy(true);
    setErr(null);
    try {
      const r = await window.molva.createIdentity();
      setPhrase(r.mnemonic);
      setNodeId(r.nodeId);
    } catch (e) {
      setErr(String((e as Error).message || e));
    } finally {
      setBusy(false);
    }
  };

  if (!phrase) {
    return (
      <div className="onboard-choose">
        <p className="hint" style={{ textAlign: "center" }}>
          Будет сгенерирована новая фраза. Запишите её и храните в надёжном
          месте — без неё аккаунт не восстановить.
        </p>
        {err && <div className="err">{err}</div>}
        <button className="primary big" disabled={busy} onClick={() => void generate()}>
          {busy ? "генерация…" : "сгенерировать фразу"}
        </button>
        <button className="ghost" onClick={onBack}>
          назад
        </button>
      </div>
    );
  }

  const words = phrase.split(/\s+/);
  return (
    <div className="onboard-choose">
      <div className="phrase-grid">
        {words.map((w, i) => (
          <span key={i} className="phrase-word">
            <span className="phrase-num">{i + 1}</span>
            {w}
          </span>
        ))}
      </div>
      <div style={{ display: "flex", gap: 8 }}>
        <button style={{ flex: 1 }} onClick={() => void window.molva.copy(phrase)}>
          скопировать фразу
        </button>
        <button onClick={() => void window.molva.copy(nodeId)} title="Ваш NodeID">
          NodeID
        </button>
      </div>
      <label className="checkrow">
        <input type="checkbox" checked={confirmed} onChange={(e) => setConfirmed(e.target.checked)} />
        Я записал(а) фразу и понимаю, что без неё доступ не вернуть
      </label>
      <button className="primary big" disabled={!confirmed} onClick={onReady}>
        войти
      </button>
    </div>
  );
}

function Restore({ onReady, onBack }: { onReady: () => void; onBack: () => void }) {
  const [value, setValue] = useState("");
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    setBusy(true);
    setErr(null);
    try {
      await window.molva.restoreIdentity(value.trim());
      onReady();
    } catch (e) {
      setErr(String((e as Error).message || e));
    } finally {
      setBusy(false);
    }
  };

  const count = value.trim() ? value.trim().split(/\s+/).length : 0;
  return (
    <div className="onboard-choose">
      <p className="hint" style={{ textAlign: "center" }}>
        Введите свою фразу из 24 слов (через пробел).
      </p>
      <textarea
        autoFocus
        rows={4}
        value={value}
        placeholder="word1 word2 word3 … word24"
        onChange={(e) => {
          setValue(e.target.value);
          setErr(null);
        }}
        style={{ width: "100%", fontFamily: "var(--mono)", fontSize: 13 }}
      />
      <div className="hint" style={{ fontSize: 11, color: count === 24 ? "var(--lamp)" : "var(--faint)" }}>
        слов: {count} / 24
      </div>
      {err && <div className="err">{err}</div>}
      <button className="primary big" disabled={count !== 24 || busy} onClick={() => void submit()}>
        {busy ? "восстановление…" : "восстановить"}
      </button>
      <button className="ghost" onClick={onBack}>
        назад
      </button>
    </div>
  );
}
