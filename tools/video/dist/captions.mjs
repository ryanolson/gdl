export function captionAt(turn, time) {
  if (!turn || time < turn.start || time >= turn.end) return '';
  const words = turn.caption.trim().split(/\s+/);
  const chunks = [];
  for (let start = 0; start < words.length; start += 12) {
    chunks.push({ text: words.slice(start, start + 12).join(' '), start, end: Math.min(words.length, start + 12) });
  }
  const word = (time - turn.start) / (turn.end - turn.start) * words.length;
  return chunks.find(chunk => word >= chunk.start && word < chunk.end)?.text ?? '';
}
