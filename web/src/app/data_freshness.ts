const MANIFEST_URL =
  'https://huggingface.co/datasets/vladimir-io/healthspend-data/resolve/main/dataset_manifest.json';

function formatFreshness(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '';
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

/** Show when hospital price data was last refreshed (from published dataset manifest). */
export function setupDataFreshness(): void {
  const pill = document.getElementById('hero-status-pill');
  if (!pill) return;

  void fetch(MANIFEST_URL, { cache: 'no-store' })
    .then((r) => (r.ok ? r.json() : null))
    .then((data: { generated_at?: string } | null) => {
      if (!data?.generated_at) return;
      const label = formatFreshness(data.generated_at);
      if (!label) return;
      pill.textContent = `7,400+ hospitals · data updated ${label}`;
    })
    .catch(() => undefined);
}
