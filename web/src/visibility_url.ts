/** Crawlable hospital landing page for a CMS CCN. */
export function hospitalVisibilityPath(ccn: string | number | null | undefined): string | null {
  const digits = String(ccn ?? '').replace(/\D/g, '');
  if (digits.length < 4) return null;
  const normalized = digits.length > 6 ? digits.slice(-6) : digits;
  return `/visibility/node-${normalized.padStart(6, '0')}.html`;
}
