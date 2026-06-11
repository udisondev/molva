// Preload: единственный мост renderer ↔ main. Renderer получает реквизиты
// WebSocket-подключения к ядру и буфер обмена; больше ничего из Node.
import { contextBridge, ipcRenderer } from "electron";

contextBridge.exposeInMainWorld("molva", {
  conn: (): Promise<{ addr: string; token: string }> => ipcRenderer.invoke("molva:conn"),
  copy: (text: string): Promise<void> => ipcRenderer.invoke("molva:copy", text),
  pickFile: (): Promise<string | null> => ipcRenderer.invoke("molva:pickFile"),
});
