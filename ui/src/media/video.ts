// Видеопуть звонка: getUserMedia(video) → VideoEncoder (VP8) → IPC-кадр
// канала 17 (кадр целиком; дробление на сегменты делает ядро). Приём —
// VideoDecoder → canvas. Лестница пресетов управляется ядром: событие
// MediaPreset перенастраивает энкодер (0 — видео выключено).
import { MolvaClient } from "../ipc/client";

const VIDEO_CHANNEL = 17;

export interface VideoPresetCfg {
  width: number;
  height: number;
  bitrate: number;
  framerate: number;
}

// Ступени лестницы качества (индекс = уровень из события MediaPreset).
export const PRESETS: (VideoPresetCfg | null)[] = [
  null, // 0: только аудио
  { width: 320, height: 240, bitrate: 250_000, framerate: 15 },
  { width: 640, height: 480, bitrate: 700_000, framerate: 24 },
  { width: 1280, height: 720, bitrate: 1_800_000, framerate: 30 },
];

export class CallVideo {
  private client: MolvaClient;
  private stream: MediaStream | null = null;
  private encoder: VideoEncoder | null = null;
  private decoder: VideoDecoder | null = null;
  private canvas: HTMLCanvasElement | null = null;
  private running = false;
  private frameCounter = 0;

  constructor(client: MolvaClient) {
    this.client = client;
  }

  // attachCanvas — куда рисовать удалённое видео.
  attachCanvas(canvas: HTMLCanvasElement): void {
    this.canvas = canvas;
  }

  // onMediaFrame — входящий собранный видеокадр от ядра (канал 17).
  onMediaFrame(payload: Uint8Array): void {
    if (!this.decoder || this.decoder.state !== "configured") return;
    this.decoder.decode(
      new EncodedVideoChunk({
        // Ключевая инфа о типе кадра в VP8-потоке лежит в самом битстриме;
        // декодер VP8 переживает пометку delta после первого ключевого.
        type: this.frameCounter++ === 0 ? "key" : "delta",
        timestamp: 0,
        data: payload.slice(),
      }),
    );
  }

  // setPreset перенастраивает энкодер; level 0 гасит видео.
  async setPreset(level: number): Promise<void> {
    const cfg = PRESETS[level] ?? null;
    if (!cfg) {
      this.stopCapture();
      return;
    }
    if (!this.running) await this.startCapture(cfg);
    this.encoder?.configure({
      codec: "vp8",
      width: cfg.width,
      height: cfg.height,
      bitrate: cfg.bitrate,
      framerate: cfg.framerate,
      latencyMode: "realtime",
    });
  }

  private async startCapture(cfg: VideoPresetCfg): Promise<void> {
    this.running = true;
    this.decoder = new VideoDecoder({
      output: (frame) => this.draw(frame),
      error: () => {},
    });
    this.decoder.configure({ codec: "vp8" });

    this.encoder = new VideoEncoder({
      output: (chunk) => {
        const buf = new Uint8Array(chunk.byteLength);
        chunk.copyTo(buf);
        this.client.sendMedia(VIDEO_CHANNEL, buf);
      },
      error: () => {},
    });

    this.stream = await navigator.mediaDevices.getUserMedia({
      video: { width: cfg.width, height: cfg.height },
    });
    const [track] = this.stream.getVideoTracks();
    const processor = new MediaStreamTrackProcessor<VideoFrame>({ track });
    const reader = processor.readable.getReader();
    void (async () => {
      let n = 0;
      for (;;) {
        const { done, value } = await reader.read();
        if (done || !this.running) {
          value?.close();
          return;
        }
        if (this.encoder && this.encoder.state === "configured") {
          // Ключевой кадр раз в ~2 секунды.
          this.encoder.encode(value, { keyFrame: n++ % 60 === 0 });
        }
        value.close();
      }
    })();
  }

  private draw(frame: VideoFrame): void {
    const canvas = this.canvas;
    if (!canvas) {
      frame.close();
      return;
    }
    canvas.width = frame.displayWidth;
    canvas.height = frame.displayHeight;
    canvas.getContext("2d")?.drawImage(frame, 0, 0);
    frame.close();
  }

  private stopCapture(): void {
    this.running = false;
    this.stream?.getTracks().forEach((t) => t.stop());
    this.stream = null;
    this.encoder?.close();
    this.encoder = null;
    this.decoder?.close();
    this.decoder = null;
    this.frameCounter = 0;
  }

  stop(): void {
    this.stopCapture();
  }
}
