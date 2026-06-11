// Дымовая проверка протокола: подключение к живому molvad, Hello с
// токеном, MyInvite и ListChats через сгенерированный TS-кодек.
// Запуск: node dist-smoke/smoke.js <addr> <token-hex>
import { Command, CommandResult, Frame, Hello } from "../src/gen/ipc";

const [addr, tokenHex] = process.argv.slice(2);
if (!addr || !tokenHex) {
  console.error("usage: smoke <addr> <token-hex>");
  process.exit(2);
}

function hexToBytes(hex: string): Uint8Array {
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}

const ws = new WebSocket(`ws://${addr}`);
ws.binaryType = "arraybuffer";

const pending = new Map<bigint, (r: CommandResult) => void>();
let nextId = 1n;

function command(kind: NonNullable<Command["kind"]>): Promise<CommandResult> {
  const id = nextId++;
  ws.send(Frame.encode({ kind: { $case: "command", command: Command.create({ id, kind }) } }).finish());
  return new Promise((res) => pending.set(id, res));
}

ws.onopen = async () => {
  ws.send(
    Frame.encode({
      kind: { $case: "hello", hello: Hello.create({ token: hexToBytes(tokenHex) }) },
    }).finish(),
  );

  const inv = await command({ $case: "myInvite", myInvite: { alias: "Дым" } });
  if (inv.error || inv.data?.$case !== "invite" || !inv.data.invite.invite.startsWith("molva://add/")) {
    console.error("MyInvite сломан:", inv);
    process.exit(1);
  }
  console.log("invite:", inv.data.invite.invite.slice(0, 40) + "…");

  const chats = await command({ $case: "listChats", listChats: {} });
  if (chats.error || chats.data?.$case !== "chats") {
    console.error("ListChats сломан:", chats);
    process.exit(1);
  }
  console.log("chats:", chats.data.chats.chats.length);
  console.log("SMOKE-OK");
  process.exit(0);
};

ws.onmessage = (m) => {
  const f = Frame.decode(new Uint8Array(m.data as ArrayBuffer));
  if (f.kind?.$case !== "event") return;
  const ev = f.kind.event;
  if (ev.kind?.$case === "commandResult") {
    const cb = pending.get(ev.kind.commandResult.id);
    if (cb) {
      pending.delete(ev.kind.commandResult.id);
      cb(ev.kind.commandResult);
    }
  }
};

ws.onerror = () => {
  console.error("ws error");
  process.exit(1);
};

setTimeout(() => {
  console.error("таймаут");
  process.exit(1);
}, 10000);
