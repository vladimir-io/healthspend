import type { AppRoute } from './nav';

const loaded = new Set<AppRoute>();

export async function ensureDeferredView(route: AppRoute): Promise<void> {
  if (route === 'search' || loaded.has(route)) return;

  if (route === 'hospitals') {
    const { renderHospitals } = await import('../views/hospitals');
    await renderHospitals('view-hospitals');
    loaded.add('hospitals');
    return;
  }

  if (route === 'methodology') {
    const { renderMethodology } = await import('../views/methodology');
    await renderMethodology('view-methodology');
    loaded.add('methodology');
  }
}
