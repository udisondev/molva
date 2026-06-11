// Мост preload: реквизиты подключения к ядру и буфер обмена.
interface Window {
  molva: {
    conn(): Promise<{ addr: string; token: string }>;
    copy(text: string): Promise<void>;
    pickFile(): Promise<string | null>;
  };
}

// MediaStreamTrackProcessor — API Chromium (есть в Electron), отсутствует
// в стандартных DOM-типах.
declare class MediaStreamTrackProcessor<T> {
  constructor(init: { track: MediaStreamTrack });
  readable: ReadableStream<T>;
}
