// Клиент IPC: один WebSocket к molvad, бинарные protobuf-кадры.
// Команды коррелируются по id, события уходят в колбэк. Renderer не несёт
// бизнес-логики — только зеркалит состояние ядра.
import { Command, CommandResult, Event as CoreEvent, Frame, Hello } from "../gen/ipc";

export type EventHandler = (ev: CoreEvent) => void;
export type StatusHandler = (connected: boolean) => void;
export type MediaHandler = (ch: number, rxMicros: bigint, payload: Uint8Array) => void;

// Теги кадров WS: первый байт.
const TAG_PROTO = 0x00;
const TAG_MEDIA = 0x01;

function wrapProto(b: Uint8Array): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(b.length + 1);
  out[0] = TAG_PROTO;
  out.set(b, 1);
  return out;
}

const RECONNECT_DELAY_MS = 1500;

export class MolvaClient {
  private ws: WebSocket | null = null;
  private nextId = 1n;
  private pending = new Map<bigint, (r: CommandResult) => void>();
  private closed = false;

  onEvent: EventHandler = () => {};
  onStatus: StatusHandler = () => {};
  onMedia: MediaHandler = () => {};

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
            wrapProto(
              Frame.encode({
                kind: { $case: "hello", hello: Hello.create({ token: hexToBytes(token) }) },
              }).finish(),
            ),
          );
          this.onStatus(true);
        };
        ws.onmessage = (m) => {
          if (!(m.data instanceof ArrayBuffer)) return;
          const raw = new Uint8Array(m.data);
          if (raw.length === 0) return;
          if (raw[0] === TAG_MEDIA) {
            if (raw.length <= 10) return;
            const view = new DataView(raw.buffer, raw.byteOffset);
            this.onMedia(raw[1], view.getBigInt64(2), raw.subarray(10));
            return;
          }
          if (raw[0] !== TAG_PROTO) return;
          let frame: Frame;
          try {
            frame = Frame.decode(raw.subarray(1));
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
    const frame = wrapProto(
      Frame.encode({
        kind: { $case: "command", command: Command.create({ id, kind }) },
      }).finish(),
    );
    return new Promise((resolve) => {
      this.pending.set(id, resolve);
      ws.send(frame);
    });
  }

  // sendMedia шлёт медиакадр звонка (канал 16 — аудио, 17 — видео).
  sendMedia(ch: number, payload: Uint8Array): void {
    const ws = this.ws;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    const out = new Uint8Array(10 + payload.length);
    out[0] = TAG_MEDIA;
    out[1] = ch;
    // rx-микросекунды значимы только для входящих; исходящие — нули.
    out.set(payload, 10);
    ws.send(out);
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
