// Сборка molvad под целевую платформу перед упаковкой: бинарь кладётся в
// build-daemon/ и уезжает в resources приложения.
import { execSync } from "node:child_process";
import { mkdirSync } from "node:fs";
import { join } from "node:path";

const out = join(import.meta.dirname, "..", "build-daemon");
mkdirSync(out, { recursive: true });
const bin = process.platform === "win32" ? "molvad.exe" : "molvad";
execSync(`go build -trimpath -ldflags=-s -o ${join(out, bin)} ./cmd/molvad`, {
  cwd: join(import.meta.dirname, "..", ".."),
  stdio: "inherit",
});
console.log("molvad собран:", join(out, bin));
