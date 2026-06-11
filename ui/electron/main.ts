// Главный процесс Electron: спавнит molvad с одноразовым auth-токеном,
// ждёт адрес IPC из его stdout и отдаёт реквизиты renderer'у по запросу.
import { app, BrowserWindow, dialog, ipcMain, clipboard } from "electron";
import { spawn, ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import { createInterface } from "node:readline";
import * as path from "node:path";

const token = randomBytes(32).toString("hex");
let daemon: ChildProcess | null = null;
let ipcAddr: Promise<string> | null = null;

// molvad ищется так: MOLVAD_BIN → рядом с приложением → в PATH.
function daemonPath(): string {
  if (process.env.MOLVAD_BIN) return process.env.MOLVAD_BIN;
  if (app.isPackaged) return path.join(process.resourcesPath, "molvad");
  return path.join(__dirname, "..", "..", "molvad");
}

function startDaemon(): Promise<string> {
  const child = spawn(daemonPath(), [], {
    env: {
      ...process.env,
      MOLVA_IPC_TOKEN: token,
      MOLVA_IPC_PORT: "0",
    },
    stdio: ["ignore", "pipe", "inherit"],
  });
  daemon = child;
  child.on("exit", (code) => {
    daemon = null;
    // Ядро умерло — без него оболочка бессмысленна.
    if (!app.isPackaged) console.error(`molvad завершился: ${code}`);
    app.quit();
  });

  return new Promise((resolve, reject) => {
    const rl = createInterface({ input: child.stdout! });
    const timer = setTimeout(() => reject(new Error("molvad не сообщил адрес IPC")), 15000);
    rl.on("line", (line) => {
      const m = line.match(/^MOLVA_IPC_ADDR=(.+)$/);
      if (m) {
        clearTimeout(timer);
        resolve(m[1]);
      }
    });
    child.on("error", (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
}

function createWindow() {
  const win = new BrowserWindow({
    width: 1100,
    height: 720,
    minWidth: 760,
    minHeight: 480,
    backgroundColor: "#0b0e10",
    title: "molva",
    webPreferences: {
      preload: path.join(__dirname, "preload.js"),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
  });
  if (process.env.VITE_DEV_SERVER_URL) {
    void win.loadURL(process.env.VITE_DEV_SERVER_URL);
  } else {
    void win.loadFile(path.join(__dirname, "..", "dist", "index.html"));
  }
}

app.whenReady().then(() => {
  ipcAddr = startDaemon();
  ipcAddr.catch((err) => {
    console.error("запуск molvad:", err);
    app.quit();
  });

  ipcMain.handle("molva:conn", async () => ({
    addr: await ipcAddr!,
    token,
  }));
  ipcMain.handle("molva:copy", (_ev, text: string) => {
    clipboard.writeText(String(text));
  });
  ipcMain.handle("molva:pickFile", async () => {
    const res = await dialog.showOpenDialog({ properties: ["openFile"] });
    return res.canceled || res.filePaths.length === 0 ? null : res.filePaths[0];
  });

  createWindow();
  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on("window-all-closed", () => {
  // Ядро переживает рестарт окна за счёт grace-периода molvad; полное
  // закрытие приложения гасит и его.
  app.quit();
});

app.on("before-quit", () => {
  daemon?.kill("SIGTERM");
});
