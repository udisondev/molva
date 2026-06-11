// Клиент IPC: один WebSocket к molvad, бинарные protobuf-кадры.
// Команды коррелируются по id, события уходят в колбэк. Renderer не несёт
// бизнес-логики — только зеркалит состояние ядра.
import { Command, CommandResult, Event as CoreEvent, Frame, Hello } from "../gen/ipc";

export type EventHandler = (ev: CoreEvent) => void;
export type StatusHandler = (connected: boolean) => void;

const RECONNECT_DELAY_MS = 1500;

export class MolvaClient {
  private ws: WebSocket | null = null;
  private nextId = 1n;
  private pending = new Map<bigint, (r: CommandResult) => void>();
  private closed = false;

  onEvent: EventHandler = () => {};
  onStatus: StatusHandler = () => {};

  start(): void {
    void this.connectLoop();
  }

  stop(): void {
    this.closed = true;
    this.ws?.close();
  }

  private async connectLoop(): Promise<void> {
    while (!this.closed) {
      try {
        await this.connectOnce();
      } catch {
        // Ядро ещё поднимается или перезапускается — пробуем снова.
      }
      this.onStatus(false);
      if (this.closed) return;
      await new Promise((r) => setTimeout(r, RECONNECT_DELAY_MS));
    }
  }

  private connectOnce(): Promise<void> {
    return new Promise((resolve, reject) => {
      void window.molva.conn().then(({ addr, token }) => {
        const ws = new WebSocket(`ws://${addr}`);
        ws.binaryType = "arraybuffer";
        this.ws = ws;

        ws.onopen = () => {
          ws.send(
            Frame.encode({
              kind: { $case: "hello", hello: Hello.create({ token: hexToBytes(token) }) },
            }).finish(),
          );
          this.onStatus(true);
        };
        ws.onmessage = (m) => {
          if (!(m.data instanceof ArrayBuffer)) return;
          let frame: Frame;
          try {
            frame = Frame.decode(new Uint8Array(m.data));
          } catch {
            return;
          }
          if (frame.kind?.$case !== "event") return;
          const ev = frame.kind.event;
          if (ev.kind?.$case === "commandResult") {
            const res = ev.kind.commandResult;
            const cb = this.pending.get(res.id);
            if (cb) {
              this.pending.delete(res.id);
              cb(res);
              return;
            }
          }
          this.onEvent(ev);
        };
        ws.onclose = () => {
          for (const cb of this.pending.values()) {
            cb(CommandResult.create({ error: "соединение с ядром потеряно" }));
          }
          this.pending.clear();
          resolve();
        };
        ws.onerror = () => reject(new Error("ws"));
      }, reject);
    });
  }

  command(kind: NonNullable<Command["kind"]>): Promise<CommandResult> {
    const ws = this.ws;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      return Promise.resolve(CommandResult.create({ error: "нет соединения с ядром" }));
    }
    const id = this.nextId++;
    const frame = Frame.encode({
      kind: { $case: "command", command: Command.create({ id, kind }) },
    }).finish();
    return new Promise((resolve) => {
      this.pending.set(id, resolve);
      ws.send(frame);
    });
  }
}

export function hexToBytes(hex: string): Uint8Array {
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

export function bytesToHex(b: Uint8Array): string {
  return Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
}
