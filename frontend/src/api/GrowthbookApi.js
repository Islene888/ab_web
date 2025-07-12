// src/api/GrowthbookApi.js

export async function fetchExperiments() {
  const res = await fetch('/api/experiments');
  if (!res.ok) {
    console.error('Failed to fetch experiments', res.status, res.statusText);
    return [];
  }
  return res.json();
}
