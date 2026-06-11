import { useEffect, useState } from "react";
import { App } from "./App";
import { Onboarding } from "./components/Onboarding";
import { startClient } from "./state/store";

// Гейт первого экрана: пока неизвестно состояние — заглушка; нет личности —
// онбординг; есть — поднимаем клиента ядра и показываем мессенджер.
export function Root() {
  const [phase, setPhase] = useState<"loading" | "onboarding" | "ready">("loading");

  useEffect(() => {
    void window.molva.status().then((s) => {
      if (s.hasIdentity) {
        startClient();
        setPhase("ready");
      } else {
        setPhase("onboarding");
      }
    });
  }, []);

  if (phase === "loading") {
    return (
      <div className="onboard">
        <div className="wordmark" style={{ fontSize: 18, letterSpacing: "0.4em" }}>
          MOLVA
        </div>
      </div>
    );
  }
  if (phase === "onboarding") {
    return (
      <Onboarding
        onReady={() => {
          startClient();
          setPhase("ready");
        }}
      />
    );
  }
  return <App />;
}
