// Позывной-аватар: две литеры на плашке, оттенок выводится из NodeID —
// у каждого корреспондента свой стабильный цвет, без картинок.
export function Avatar({ peerHex, alias }: { peerHex: string; alias: string }) {
  const hue = (parseInt(peerHex.slice(0, 6), 16) % 360 + 360) % 360;
  const letters = (alias.trim() || peerHex).slice(0, 2).toUpperCase();
  return (
    <div
      className="avatar"
      style={{
        background: `linear-gradient(135deg, hsl(${hue} 45% 62%), hsl(${(hue + 40) % 360} 50% 48%))`,
      }}
    >
      {letters}
    </div>
  );
}
