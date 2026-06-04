/** Human-readable UI copy (sentence case, not terminal shouting). */

export const FALLBACK_LABELS: Record<string, string> = {
  state_fuzzy: 'Expanded within your state',
  national_scope: 'Showing national results',
  zip_relaxed_national: 'ZIP widened to national',
  national_text_match: 'Matched by procedure text',
  category_fallback: 'Similar procedure category',
};

export function formatResultsSummary(parts: {
  showing?: string;
  total?: number;
  confidence?: string;
  fallback?: boolean;
  queryMs?: number;
}): string {
  const bits: string[] = [];
  if (parts.showing) bits.push(parts.showing);
  else if (parts.total != null) bits.push(`${parts.total.toLocaleString()} hospitals`);
  if (parts.confidence) bits.push(parts.confidence);
  if (parts.fallback) bits.push('Broader match applied');
  if (parts.queryMs != null) bits.push(`${Math.round(parts.queryMs)} ms`);
  return bits.join(' · ');
}
