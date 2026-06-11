// Главный процесс Electron: владеет каталогом данных и жизненным циклом
// личности. Онбординг (создать/восстановить seed) идёт короткими
// подкомандами molvad ДО запуска долгоживущего узла; смена бутстрапа
// пишет файл и перезапускает узел (renderer переподключается сам).
import { app, BrowserWindow, dialog, ipcMain, clipboard } from "electron";
import { spawn, ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import { createInterface } from "node:readline";
import { existsSync } from "node:fs";
import * as path from "node:path";

const token = randomBytes(32).toString("hex");
let dataDir = "";
let daemon: ChildProcess | null = null;
let ipcAddr: Promise<string> | null = null;
let quitting = false;

function daemonPath(): string {
  if (process.env.MOLVAD_BIN) return process.env.MOLVAD_BIN;
  if (app.isPackaged) return path.join(process.resourcesPath, "molvad");
  return path.join(__dirname, "..", "..", "molvad");
}

function seedPath(): string {
  return path.join(dataDir, "molva.seed");
}

// runCmd запускает короткую подкоманду molvad, кормит stdin (если задан),
// собирает stdout как карту KEY=value; ошибку берёт из stderr.
function runCmd(args: string[], stdin?: string): Promise<Record<string, string>> {
  return new Promise((resolve, reject) => {
    const child = spawn(daemonPath(), [...args, "-data", dataDir], {
      stdio: ["pipe", "pipe", "pipe"],
    });
    let out = "";
    let err = "";
    child.stdout.on("data", (d) => (out += d));
    child.stderr.on("data", (d) => (err += d));
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code !== 0) {
        const m = err.match(/err="([^"]+)"/);
        reject(new Error(m ? m[1] : err.trim() || `molvad exit ${code}`));
        return;
      }
      const map: Record<string, string> = {};
      for (const line of out.split("\n")) {
        const eq = line.indexOf("=");
        if (eq > 0) map[line.slice(0, eq)] = line.slice(eq + 1).trim();
      }
      resolve(map);
    });
    if (stdin !== undefined) {
      child.stdin.write(stdin);
    }
    child.stdin.end();
  });
}

// startDaemon поднимает долгоживущий узел (личность уже должна быть на
// диске) и резолвит адрес IPC из его stdout.
function startDaemon(): Promise<string> {
  if (ipcAddr) return ipcAddr;
  const child = spawn(daemonPath(), ["-data", dataDir, "-grace", "0"], {
    env: { ...process.env, MOLVA_IPC_TOKEN: token, MOLVA_IPC_PORT: "0" },
    stdio: ["ignore", "pipe", "inherit"],
  });
  daemon = child;
  child.on("exit", (code) => {
    daemon = null;
    ipcAddr = null;
    if (quitting) return;
    // Ядро умерло само — без него оболочка бессмысленна.
    if (!app.isPackaged) console.error(`molvad завершился: ${code}`);
    app.quit();
  });

  ipcAddr = new Promise((resolve, reject) => {
    const rl = createInterface({ input: child.stdout! });
    const timer = setTimeout(() => reject(new Error("molvad не сообщил адрес IPC")), 15000);
    rl.on("line", (line) => {
      const m = line.match(/^MOLVA_IPC_ADDR=(.+)$/);
      if (m) {
        clearTimeout(timer);
        resolve(m[1]);
      }
    });
    child.on("error", (e) => {
      clearTimeout(timer);
      reject(e);
    });
  });
  return ipcAddr;
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
  dataDir = path.join(app.getPath("userData"), "core");

  // Состояние онбординга: есть ли личность на диске.
  ipcMain.handle("molva:status", () => ({ hasIdentity: existsSync(seedPath()) }));

  // Онбординг (личность ещё не запущена).
  ipcMain.handle("molva:createIdentity", async () => {
    const r = await runCmd(["-gen-seed"]);
    return { nodeId: r.NODEID, mnemonic: r.MNEMONIC };
  });
  ipcMain.handle("molva:restoreIdentity", async (_ev, mnemonic: string) => {
    const r = await runCmd(["-restore-seed"], mnemonic);
    return { nodeId: r.NODEID };
  });
  ipcMain.handle("molva:exportMnemonic", async () => {
    const r = await runCmd(["-show-mnemonic"]);
    return r.MNEMONIC;
  });

  // Запуск/реквизиты узла (после онбординга).
  ipcMain.handle("molva:start", async () => {
    if (!existsSync(seedPath())) throw new Error("личность ещё не создана");
    return { addr: await startDaemon(), token };
  });
  ipcMain.handle("molva:conn", async () => ({ addr: await startDaemon(), token }));

  // Бутстрап — целиком на стороне ядра (live add через node.Bootstrap,
  // remove — в файл на следующий старт); см. команды IPC ListBootstrap и пр.

  ipcMain.handle("molva:copy", (_ev, text: string) => clipboard.writeText(String(text)));
  ipcMain.handle("molva:pickFile", async () => {
    const res = await dialog.showOpenDialog({ properties: ["openFile"] });
    return res.canceled || res.filePaths.length === 0 ? null : res.filePaths[0];
  });

  createWindow();
  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on("window-all-closed", () => app.quit());
app.on("before-quit", () => {
  quitting = true;
  daemon?.kill("SIGTERM");
});
