// Аудиопуть звонка в renderer'е: getUserMedia → AudioEncoder (Opus,
// кадр 20 мс, mono 32 kbps) → IPC-датаграммы (канал 16); приём — декод и
// планирование через WebAudio с целевым буфером ~120 мс (джиттер-буфер).
// Потерянный кадр не ретраится — упругость даёт PLC Opus'а.
import { MolvaClient } from "../ipc/client";

const AUDIO_CHANNEL = 16;
const SAMPLE_RATE = 48000;
const TARGET_BUFFER_S = 0.12;

export class CallAudio {
  private client: MolvaClient;
  private stream: MediaStream | null = null;
  private encoder: AudioEncoder | null = null;
  private decoder: AudioDecoder | null = null;
  private track: MediaStreamTrackProcessor<AudioData> | null = null;
  private ctx: AudioContext | null = null;
  private playHead = 0;
  private running = false;

  constructor(client: MolvaClient) {
    this.client = client;
  }

  // start поднимает захват и кодек; зовётся при переходе звонка в active.
  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;

    this.ctx = new AudioContext({ sampleRate: SAMPLE_RATE });
    this.playHead = this.ctx.currentTime + TARGET_BUFFER_S;

    this.decoder = new AudioDecoder({
      output: (data) => this.play(data),
      error: () => {},
    });
    this.decoder.configure({
      codec: "opus",
      sampleRate: SAMPLE_RATE,
      numberOfChannels: 1,
    });
    this.client.onMedia = (ch, _rx, payload) => {
      if (ch !== AUDIO_CHANNEL || !this.decoder || this.decoder.state !== "configured") return;
      this.decoder.decode(
        new EncodedAudioChunk({
          type: "key",
          timestamp: 0,
          data: payload.slice(),
        }),
      );
    };

    this.encoder = new AudioEncoder({
      output: (chunk) => {
        const buf = new Uint8Array(chunk.byteLength);
        chunk.copyTo(buf);
        this.client.sendMedia(AUDIO_CHANNEL, buf);
      },
      error: () => {},
    });
    this.encoder.configure({
      codec: "opus",
      sampleRate: SAMPLE_RATE,
      numberOfChannels: 1,
      bitrate: 32_000,
      opus: { frameDuration: 20_000 },
    });

    this.stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    const [audioTrack] = this.stream.getAudioTracks();
    this.track = new MediaStreamTrackProcessor({ track: audioTrack });
    const reader = this.track.readable.getReader();
    void (async () => {
      for (;;) {
        const { done, value } = await reader.read();
        if (done || !this.running) {
          value?.close();
          return;
        }
        if (this.encoder && this.encoder.state === "configured") {
          this.encoder.encode(value);
        }
        value.close();
      }
    })();
  }

  // play планирует декодированный кадр; отставший от часов буфер
  // подтягивается (поздние кадры не копятся в задержку).
  private play(data: AudioData): void {
    const ctx = this.ctx;
    if (!ctx) {
      data.close();
      return;
    }
    const buf = ctx.createBuffer(1, data.numberOfFrames, data.sampleRate);
    const tmp = new Float32Array(data.numberOfFrames);
    data.copyTo(tmp, { planeIndex: 0, format: "f32-planar" });
    buf.getChannelData(0).set(tmp);
    data.close();

    const src = ctx.createBufferSource();
    src.buffer = buf;
    src.connect(ctx.destination);
    if (this.playHead < ctx.currentTime + 0.02) {
      this.playHead = ctx.currentTime + TARGET_BUFFER_S;
    }
    src.start(this.playHead);
    this.playHead += buf.duration;
  }

  stop(): void {
    this.running = false;
    this.client.onMedia = () => {};
    this.stream?.getTracks().forEach((t) => t.stop());
    this.stream = null;
    void this.encoder?.flush().catch(() => {});
    this.encoder?.close();
    this.encoder = null;
    this.decoder?.close();
    this.decoder = null;
    void this.ctx?.close();
    this.ctx = null;
  }
}
