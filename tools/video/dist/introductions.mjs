export function introductionAt(scene, time) {
  if (scene.id !== 'interruption' || time < scene.start || time >= scene.end) return null;
  return scene.introductions?.find(({ start, end }) => time >= start && time < end) ?? null;
}
