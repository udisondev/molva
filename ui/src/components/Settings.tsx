import { useEffect, useState } from "react";
import {
  addBootstrap,
  copyText,
  listBootstrap,
  myIdentity,
  removeBootstrap,
  setModal,
  showToast,
} from "../state/store";

// Настройки: точки входа сети (бутстрап), свой NodeID/адрес для шеринга,
// экспорт секретной фразы. Бутстрап — состояние ядра: добавление
// применяется на лету, удаление вступит в силу при следующем старте.
export function Settings() {
  const [lines, setLines] = useState<string[]>([]);
  const [entry, setEntry] = useState("");
  const [id, setId] = useState<{ nodeId: string; address: string } | null>(null);
  const [phrase, setPhrase] = useState<string | null>(null);

  useEffect(() => {
    void listBootstrap().then(setLines);
    void myIdentity().then(setId);
  }, []);

  const add = async () => {
    const v = entry.trim();
    if (!v) return;
    const r = await addBootstrap(v);
    if (r.error) {
      showToast(r.error.toUpperCase());
      return;
    }
    setLines(r.entries ?? []);
    setEntry("");
    showToast("ТОЧКА ВХОДА ДОБАВЛЕНА");
  };

  const remove = async (l: string) => {
    setLines(await removeBootstrap(l));
    showToast("УДАЛЕНО — ВСТУПИТ В СИЛУ ПРИ СЛЕДУЮЩЕМ СТАРТЕ");
  };

  const shareLine = id?.address ? `${id.nodeId}@${id.address}` : id?.nodeId ?? "";

  return (
    <div className="backdrop" onClick={() => setModal(null)}>
      <div className="modal" style={{ width: 560 }} onClick={(e) => e.stopPropagation()}>
        <h2>Настройки</h2>

        <section>
          <label style={labelStyle}>ТОЧКИ ВХОДА СЕТИ (БУТСТРАП)</label>
          <div className="hint" style={{ marginBottom: 8 }}>
            Узлы, через которые вы входите в сеть. Добавление действует сразу;
            удаление — со следующего запуска (живой узел уже изучил топологию).
          </div>
          <div className="boot-list">
            {lines.length === 0 && <div className="hint">список пуст</div>}
            {lines.map((l) => (
              <div key={l} className="boot-row">
                <span className="boot-addr">{l}</span>
                <button className="ghost" onClick={() => void remove(l)} title="Удалить">
                  ✕
                </button>
              </div>
            ))}
          </div>
          <div style={{ display: "flex", gap: 6, marginTop: 8 }}>
            <input
              value={entry}
              placeholder="hexid@host:port"
              onChange={(e) => setEntry(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && void add()}
              style={{ flex: 1, fontFamily: "var(--mono)", fontSize: 12 }}
            />
            <button onClick={() => void add()}>добавить</button>
          </div>
        </section>

        <section>
          <label style={labelStyle}>МОЯ ТОЧКА ВХОДА (для других)</label>
          <div className="hint" style={{ marginBottom: 6 }}>
            {id?.address
              ? "Передайте эту строку тем, кто хочет бутстрапнуться на вас."
              : "Внешний адрес ещё не подтверждён — пока доступен только NodeID."}
          </div>
          <div className="fullid" title="Скопировать" onClick={() => shareLine && void copyText(shareLine)}>
            {shareLine || "…"}
          </div>
        </section>

        <section>
          <label style={labelStyle}>СЕКРЕТНАЯ ФРАЗА (БЭКАП)</label>
          {phrase ? (
            <div className="phrase-grid small">
              {phrase.split(/\s+/).map((w, i) => (
                <span key={i} className="phrase-word">
                  <span className="phrase-num">{i + 1}</span>
                  {w}
                </span>
              ))}
            </div>
          ) : (
            <button
              className="danger"
              onClick={() => void window.molva.exportMnemonic().then(setPhrase)}
            >
              показать фразу
            </button>
          )}
          {phrase && (
            <div style={{ display: "flex", gap: 8, marginTop: 8 }}>
              <button onClick={() => void copyText(phrase, "ФРАЗА СКОПИРОВАНА")}>скопировать</button>
              <button className="ghost" onClick={() => setPhrase(null)}>
                скрыть
              </button>
            </div>
          )}
        </section>

        <div className="row">
          <button onClick={() => setModal(null)}>закрыть</button>
        </div>
      </div>
    </div>
  );
}

const labelStyle: React.CSSProperties = {
  fontFamily: "var(--mono)",
  fontSize: 10,
  letterSpacing: "0.2em",
  color: "var(--amber)",
  display: "block",
  marginBottom: 6,
};
