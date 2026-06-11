// Стор renderer'а: зеркало состояния ядра + UI-состояние (выбор, модалки).
// Источник правды — molvad; события дополняют, запросы пересинхронизируют.
import { useSyncExternalStore } from "react";
import { MolvaClient, bytesToHex } from "../ipc/client";
import { Chat_State, Message, Message_Status, Event as CoreEvent } from "../gen/ipc";

export interface MessageVM {
  key: string; // hex msg_id
  msgId: Uint8Array;
  peerHex: string;
  outgoing: boolean;
  text: string;
  deleted: boolean;
  sentAt: number;
  status: Message_Status;
}

export interface ChatVM {
  peerHex: string;
  peer: Uint8Array;
  alias: string;
  online: boolean;
  state: Chat_State;
  preview: string;
  previewAt: number;
  unread: number;
}

export interface State {
  connected: boolean;
  chats: ChatVM[];
  threads: Record<string, MessageVM[]>;
  selected: string | null;
  drawer: boolean;
  modal: "invite" | "add" | null;
  invite: string;
  toast: string | null;
}

let state: State = {
  connected: false,
  chats: [],
  threads: {},
  selected: null,
  drawer: false,
  modal: null,
  invite: "",
  toast: null,
};

const listeners = new Set<() => void>();

function setState(patch: Partial<State>): void {
  state = { ...state, ...patch };
  for (const l of listeners) l();
}

export function useStore(): State {
  return useSyncExternalStore(
    (cb) => {
      listeners.add(cb);
      return () => listeners.delete(cb);
    },
    () => state,
  );
}

export const client = new MolvaClient();

function toVM(m: Message): MessageVM {
  return {
    key: bytesToHex(m.msgId),
    msgId: m.msgId,
    peerHex: bytesToHex(m.peer),
    outgoing: m.outgoing,
    text: m.text,
    deleted: m.deleted,
    sentAt: Number(m.sentAt),
    status: m.status,
  };
}

function preview(m: Message | undefined): { text: string; at: number } {
  if (!m) return { text: "", at: 0 };
  if (m.deleted) return { text: "‹стёрто›", at: Number(m.sentAt) };
  return { text: (m.outgoing ? "↗ " : "") + m.text, at: Number(m.sentAt) };
}

export async function refreshChats(): Promise<void> {
  const res = await client.command({ $case: "listChats", listChats: {} });
  if (res.error || res.data?.$case !== "chats") return;
  const prev = new Map(state.chats.map((c) => [c.peerHex, c]));
  const chats = res.data.chats.chats.map((c): ChatVM => {
    const hex = bytesToHex(c.peer);
    const p = preview(c.lastMessage);
    return {
      peerHex: hex,
      peer: c.peer,
      alias: c.alias,
      online: c.online,
      state: c.state,
      preview: p.text,
      previewAt: p.at,
      unread: prev.get(hex)?.unread ?? 0,
    };
  });
  chats.sort((a, b) => b.previewAt - a.previewAt);
  setState({ chats });
}

export async function openThread(peerHex: string): Promise<void> {
  const chat = state.chats.find((c) => c.peerHex === peerHex);
  setState({
    selected: peerHex,
    drawer: false,
    chats: state.chats.map((c) => (c.peerHex === peerHex ? { ...c, unread: 0 } : c)),
  });
  if (!chat) return;
  const res = await client.command({
    $case: "listMessages",
    listMessages: { peer: chat.peer, beforeSeq: 0n, limit: 200 },
  });
  if (res.error || res.data?.$case !== "messages") return;
  setState({
    threads: { ...state.threads, [peerHex]: res.data.messages.messages.map(toVM) },
  });
}

export async function sendText(text: string): Promise<void> {
  const sel = state.selected;
  const chat = state.chats.find((c) => c.peerHex === sel);
  if (!sel || !chat) return;
  const res = await client.command({
    $case: "sendText",
    sendText: { peer: chat.peer, text },
  });
  if (res.error) {
    showToast(res.error);
    return;
  }
  if (res.data?.$case === "sent" && res.data.sent.message) {
    appendMessage(toVM(res.data.sent.message));
  }
}

export async function deleteMessage(m: MessageVM): Promise<void> {
  const chat = state.chats.find((c) => c.peerHex === m.peerHex);
  if (!chat) return;
  const res = await client.command({
    $case: "deleteMessage",
    deleteMessage: { peer: chat.peer, msgId: m.msgId },
  });
  if (res.error) {
    showToast(res.error);
    return;
  }
  const thread = (state.threads[m.peerHex] ?? []).map((x) =>
    x.key === m.key ? { ...x, deleted: true, text: "" } : x,
  );
  setState({ threads: { ...state.threads, [m.peerHex]: thread } });
  void refreshChats();
}

export async function addContact(invite: string): Promise<string | null> {
  const res = await client.command({ $case: "addContact", addContact: { invite } });
  if (res.error) return res.error;
  await refreshChats();
  return null;
}

export async function acceptContact(c: ChatVM): Promise<void> {
  await client.command({ $case: "acceptContact", acceptContact: { peer: c.peer } });
  await refreshChats();
}

export async function rejectContact(c: ChatVM): Promise<void> {
  await client.command({ $case: "rejectContact", rejectContact: { peer: c.peer } });
  await refreshChats();
}

export async function blockContact(c: ChatVM): Promise<void> {
  await client.command({ $case: "blockContact", blockContact: { peer: c.peer } });
  await refreshChats();
}

export async function unblockContact(c: ChatVM): Promise<void> {
  await client.command({ $case: "unblockContact", unblockContact: { peer: c.peer } });
  await refreshChats();
}

export async function setAlias(c: ChatVM, alias: string): Promise<void> {
  await client.command({ $case: "setAlias", setAlias: { peer: c.peer, alias } });
  await refreshChats();
}

export async function loadInvite(alias: string): Promise<void> {
  const res = await client.command({ $case: "myInvite", myInvite: { alias } });
  if (res.data?.$case === "invite") {
    setState({ invite: res.data.invite.invite });
  }
}

export function selectChat(peerHex: string | null): void {
  if (peerHex) void openThread(peerHex);
  else setState({ selected: null });
}

export function setModal(modal: State["modal"]): void {
  setState({ modal, invite: modal === "invite" ? state.invite : "" });
}

export function toggleDrawer(): void {
  setState({ drawer: !state.drawer });
}

let toastTimer: ReturnType<typeof setTimeout> | null = null;
export function showToast(text: string): void {
  setState({ toast: text });
  if (toastTimer) clearTimeout(toastTimer);
  toastTimer = setTimeout(() => setState({ toast: null }), 1800);
}

export async function copyText(text: string, note = "СКОПИРОВАНО"): Promise<void> {
  await window.molva.copy(text);
  showToast(note);
}

function appendMessage(m: MessageVM): void {
  const thread = state.threads[m.peerHex];
  const next = thread ? [...thread.filter((x) => x.key !== m.key), m] : [m];
  setState({ threads: { ...state.threads, [m.peerHex]: next } });
}

function handleEvent(ev: CoreEvent): void {
  switch (ev.kind?.$case) {
    case "messageReceived": {
      const pm = ev.kind.messageReceived.message;
      if (!pm) break;
      const m = toVM(pm);
      if (state.threads[m.peerHex] || state.selected === m.peerHex) appendMessage(m);
      const chats = state.chats.map((c) =>
        c.peerHex === m.peerHex
          ? {
              ...c,
              preview: m.text,
              previewAt: m.sentAt,
              unread: state.selected === m.peerHex ? 0 : c.unread + 1,
            }
          : c,
      );
      chats.sort((a, b) => b.previewAt - a.previewAt);
      setState({ chats });
      break;
    }
    case "messageDelivered": {
      const { peer, msgId } = ev.kind.messageDelivered;
      const peerHex = bytesToHex(peer);
      const key = bytesToHex(msgId);
      const thread = state.threads[peerHex];
      if (thread) {
        setState({
          threads: {
            ...state.threads,
            [peerHex]: thread.map((m) =>
              m.key === key ? { ...m, status: Message_Status.STATUS_DELIVERED } : m,
            ),
          },
        });
      }
      break;
    }
    case "presenceChanged": {
      const { peer, online } = ev.kind.presenceChanged;
      const peerHex = bytesToHex(peer);
      setState({
        chats: state.chats.map((c) => (c.peerHex === peerHex ? { ...c, online } : c)),
      });
      break;
    }
    case "contactRequested":
    case "contactAccepted":
      void refreshChats();
      break;
    case "fileOffered": {
      const f = ev.kind.fileOffered;
      showToast(`ПРИЁМ ФАЙЛА: ${f.name}`);
      break;
    }
    case "fileDone": {
      showToast(`ФАЙЛ ПРИНЯТ: ${ev.kind.fileDone.path}`);
      break;
    }
  }
}

// offerFile предлагает файл текущему собеседнику.
export async function offerFile(): Promise<void> {
  const sel = state.selected;
  const chat = state.chats.find((c) => c.peerHex === sel);
  if (!chat) return;
  const path = await window.molva.pickFile();
  if (!path) return;
  const res = await client.command({
    $case: "offerFile",
    offerFile: { peer: chat.peer, path },
  });
  showToast(res.error ? res.error : "ФАЙЛ ПРЕДЛОЖЕН");
}

export function startClient(): void {
  client.onEvent = handleEvent;
  client.onStatus = (connected) => {
    setState({ connected });
    if (connected) void refreshChats();
  };
  client.start();
}
