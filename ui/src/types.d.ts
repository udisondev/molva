// Мост preload: онбординг личности, подключение к ядру, бутстрап, буфер.
interface Window {
  molva: {
    status(): Promise<{ hasIdentity: boolean }>;
    createIdentity(): Promise<{ nodeId: string; mnemonic: string }>;
    restoreIdentity(mnemonic: string): Promise<{ nodeId: string }>;
    exportMnemonic(): Promise<string>;
    start(): Promise<{ addr: string; token: string }>;
    conn(): Promise<{ addr: string; token: string }>;
    copy(text: string): Promise<void>;
    pickFile(): Promise<string | null>;
  };
}

// CSS-импорты обрабатывает vite, для tsc это сторонние модули без типов.
declare module "*.css";

// MediaStreamTrackProcessor — API Chromium (есть в Electron), отсутствует
// в стандартных DOM-типах.
declare class MediaStreamTrackProcessor<T> {
  constructor(init: { track: MediaStreamTrack });
  readable: ReadableStream<T>;
}
