/** Declarative search quality expectations — run via `npm run search:quality`. */

export type SearchQualityCase = {
  name: string;
  query: string;
  state?: string;
  zip?: string;
  minRows: number;
  /** Resolved CPT codes must include every code listed (subset match). */
  expectCpts?: string[];
  /** Must not parse query as a geographic ZIP search. */
  forbidZipParse?: boolean;
  /** Substring expected in resolved display label. */
  labelIncludes?: string;
};

/** Quick-search chips on the homepage plus high-intent natural language. */
export const SEARCH_QUALITY_CASES: SearchQualityCase[] = [
  {
    name: 'heart attack (national)',
    query: 'heart attack',
    minRows: 500,
    expectCpts: ['99285', '93000'],
    labelIncludes: 'Heart attack',
  },
  {
    name: 'heart attack shortcut must not be ZIP 99285',
    query: '99285',
    minRows: 500,
    expectCpts: ['99285'],
    forbidZipParse: true,
  },
  {
    name: 'metabolic panel',
    query: '80053',
    minRows: 100,
    forbidZipParse: true,
    expectCpts: ['80053'],
  },
  { name: 'metabolic panel text', query: 'Metabolic Panel', minRows: 100, expectCpts: ['80053'] },
  { name: 'knee replacement', query: 'Knee', minRows: 50, expectCpts: ['27447'] },
  { name: 'hip replacement', query: 'Hip', minRows: 50, expectCpts: ['27130'] },
  { name: 'brain mri', query: 'MRI', minRows: 50, expectCpts: ['70551'] },
  { name: 'childbirth', query: '59400', minRows: 50, forbidZipParse: true, expectCpts: ['59400'] },
  { name: 'colonoscopy', query: 'colonoscopy', minRows: 100, expectCpts: ['45378'] },
  { name: 'emergency room', query: 'Emergency Room', minRows: 100, expectCpts: ['99283'] },
  { name: 'ct scan', query: 'CT Scan', minRows: 50, expectCpts: ['74177'] },
  { name: 'chest x-ray', query: 'X-Ray', minRows: 100, expectCpts: ['71045'] },
  { name: 'stitches', query: 'Stitches', minRows: 50, expectCpts: ['12001'] },
  { name: 'brain mri national', query: 'brain mri', minRows: 50, expectCpts: ['70551'] },
  { name: 'ct scan NJ', query: 'ct scan', state: 'NJ', minRows: 5, expectCpts: ['74177'] },
  { name: 'chest pain intent', query: 'chest pain', minRows: 200, expectCpts: ['99285', '71045'] },
];
