// Preload: единственный мост renderer ↔ main. Renderer получает реквизиты
// подключения к ядру, онбординг личности, операции бутстрапа и буфер
// обмена; больше ничего из Node.
import { contextBridge, ipcRenderer } from "electron";

contextBridge.exposeInMainWorld("molva", {
  status: (): Promise<{ hasIdentity: boolean }> => ipcRenderer.invoke("molva:status"),
  createIdentity: (): Promise<{ nodeId: string; mnemonic: string }> =>
    ipcRenderer.invoke("molva:createIdentity"),
  restoreIdentity: (mnemonic: string): Promise<{ nodeId: string }> =>
    ipcRenderer.invoke("molva:restoreIdentity", mnemonic),
  exportMnemonic: (): Promise<string> => ipcRenderer.invoke("molva:exportMnemonic"),
  start: (): Promise<{ addr: string; token: string }> => ipcRenderer.invoke("molva:start"),
  conn: (): Promise<{ addr: string; token: string }> => ipcRenderer.invoke("molva:conn"),
  copy: (text: string): Promise<void> => ipcRenderer.invoke("molva:copy", text),
  pickFile: (): Promise<string | null> => ipcRenderer.invoke("molva:pickFile"),
});
